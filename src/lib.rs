use std::{
    borrow::Borrow,
    collections::HashMap,
    hash::Hash,
    sync::{LazyLock, RwLock},
};

use crate::deserialize::value;
use deserialize::results;
use pyo3::{
    PyTypeInfo,
    prelude::*,
    sync::RwLockExt,
    types::{PyDict, PyMappingProxy},
};
use tokio::runtime::Runtime;

mod batch;
mod deserialize;
mod enums;
mod errors;
mod execution_profile;
mod policies;
mod serialize;
mod session;
mod session_builder;
mod statement;
mod types;
mod utils;

use crate::utils::add_submodule;

pub static RUNTIME: LazyLock<Runtime> = LazyLock::new(|| Runtime::new().unwrap());

/// Internal cache state enum.
///
/// Represents state of the cache, either `Active` allowing for lazy population
/// or `Frozen` with a frozen view of the cache.
enum CacheState<K, V> {
    Active {
        map: HashMap<K, Py<V>>,
    },
    Frozen {
        map: HashMap<K, Py<V>>,
        view: Py<PyMappingProxy>,
    },
}

/// Two-phase cache with lazy population and immutable freeze boundary.
///
/// # Design overview
///
/// This cache has two distinct operational phases:
///
/// ## 1. Active phase
/// - The cache behaves as a concurrent lazy-initialized map
/// - Keys may be inserted on-demand via `get_or_init`
/// - Missing values are computed using `f` closure
/// - Computation is intentionally performed outside locks to reduce contention
///
/// ## 2. Frozen phase
/// - The cache becomes immutable in terms of key domain
/// - No new keys may be inserted
/// - A stable Python `MappingProxy` view is created and returned
/// - All subsequent reads observe a fixed snapshot of the cache
///
/// # Dual representation model
///
/// After freeze two synchronized representations are maintained:
///
/// - **Rust HashMap<K, Py<V>>**
///   - Internal authoritative storage
///   - Used for fast lookups and interop
///
/// - **Python MappingProxy**
///   - Immutable external snapshot
///   - Safe to expose across Python boundaries
///
/// # Concurrency model
///
/// - Uses `RwLock` for synchronization
/// - Read path is lock-shared and fast
/// - Write path re-validates state after lock acquisition
/// - Computation is performed outside locks to reduce contention
///
/// # Consistency guarantees
///
/// - Active phase: eventual consistency under concurrency
/// - Frozen phase: strict immutability of keyspace
/// - Freeze acts as a one-way semantic barrier
///
/// # Tradeoffs
///
/// - May perform redundant computation under race conditions
/// - Intentionally favors throughput over avoiding duplicate work
/// - Maintains duplicate storage for correctness across Rust/Python boundary
pub struct Cache<K, V> {
    state: RwLock<CacheState<K, V>>,
}

impl<K, V> Default for Cache<K, V> {
    fn default() -> Self {
        Self::new()
    }
}

impl<K, V> Cache<K, V> {
    pub fn new() -> Self {
        Self {
            state: RwLock::new(CacheState::Active {
                map: HashMap::new(),
            }),
        }
    }

    /// Retrieves a cached value or initializes it lazily if missing.
    ///
    /// # Behavior
    ///
    /// - If key exists (Active or Frozen): returns cached `Py<V>`
    /// - If cache is Frozen and key is missing: returns `Ok(None)`
    /// - If cache is Active and key is missing:
    ///   - Calls `f` outside the lock
    ///   - Re-checks state under write lock
    ///   - Inserts value only if still absent and cache not frozen
    ///
    /// # Concurrency design
    ///
    /// This function uses an "optimistic compute" strategy:
    ///
    /// - Computation is intentionally done outside the lock
    /// - This avoids blocking other threads during expensive initialization
    /// - However, it may result in redundant computation if:
    ///   - Another thread inserts the same key
    ///   - The cache transitions to Frozen during computation
    ///
    /// In such cases, computed values are safely discarded.
    ///
    /// # Freeze interaction
    ///
    /// If the cache transitions to Frozen during execution:
    /// - No new keys will be inserted
    /// - This function will fall back to read-only behavior
    /// - Computation may still occur but will not mutate state
    ///
    /// # Tradeoff
    ///
    /// This function favors throughput and low lock contention over
    /// avoiding duplicate computation under race conditions.
    pub fn get_or_init<'py, Q, F>(&self, py: Python<'py>, key: &Q, f: F) -> PyResult<Option<Py<V>>>
    where
        K: Borrow<Q> + Eq + Hash,
        Q: Hash + Eq + ?Sized + ToOwned<Owned = K>,
        F: FnOnce(&Q) -> PyResult<Option<Py<V>>>,
    {
        {
            let read_guard = self.state.read_py_attached(py).unwrap();
            match &*read_guard {
                CacheState::Active { map } | CacheState::Frozen { map, .. } => {
                    if let Some(v) = map.get(key) {
                        return Ok(Some(v.clone_ref(py)));
                    }
                }
            }

            if matches!(&*read_guard, CacheState::Frozen { .. }) {
                return Ok(None);
            }
        }

        // Avoid work inside write lock.
        // Tradeoff wasted work for lock contention.
        let Some(resolved) = f(key)? else {
            return Ok(None);
        };
        let owned_key = key.to_owned();

        let result = {
            let mut write_guard = self.state.write_py_attached(py).unwrap();
            match &mut *write_guard {
                CacheState::Active { map } => {
                    if let Some(existing) = map.get(key) {
                        Some(existing.clone_ref(py))
                    } else {
                        map.insert(owned_key, resolved.clone_ref(py));
                        Some(resolved)
                    }
                }
                CacheState::Frozen { map, .. } => map.get(key).map(|v| v.clone_ref(py)),
            }
        };

        Ok(result)
    }

    /// Freezes the cache (if not already frozen) and returns a Python immutable mapping view.
    ///
    /// # Behavior
    ///
    /// This function performs a one-time transition from Active to Frozen:
    ///
    /// - Calls `f` (allocating a Vec) for each freeze attempt; under contention it may be
    ///   executed multiple times before one caller completes the transition to Frozen
    /// - Inserts any missing entries from the successful freeze attempt into the internal cache
    /// - Preserves already-existing values
    /// - Constructs a Python `MappingProxy` view over the final cache state
    /// - Returns the same frozen view on subsequent calls
    ///
    /// # Freeze semantics
    ///
    /// Once frozen:
    /// - The keyspace becomes immutable
    /// - No new keys may be inserted via `get_or_init`
    /// - All future reads reflect a stable snapshot of cache contents
    ///
    /// Freeze acts as a semantic barrier.
    ///
    /// # Consistency model
    ///
    /// This operation is intentionally not fully transactional:
    ///
    /// - If `f` returns an error, the operation aborts
    /// - Only successful execution results in a frozen cache
    ///
    /// # Dual representation after freeze
    ///
    /// After completion, the cache stores:
    ///
    /// - Rust `HashMap<K, Py<V>>` for internal lookups
    /// - Python `MappingProxy` as immutable external view
    ///
    /// This duplication is required because:
    /// - Python requires an immutable mapping interface
    /// - Rust requires retained ownership for access to single values.
    ///
    /// # Concurrency behavior
    ///
    /// - If already frozen, returns existing view immediately
    /// - If active, performs full transition under write lock
    ///
    /// # Tradeoffs
    ///
    /// - May duplicate work during construction phase
    /// - Builds full snapshot eagerly at freeze time
    pub fn get_or_init_python_mapping<'py, F>(
        &self,
        py: Python<'py>,
        f: F,
    ) -> PyResult<Bound<'py, PyMappingProxy>>
    where
        K: IntoPyObject<'py> + Eq + Hash + Clone,
        V: PyTypeInfo,
        F: FnOnce() -> Vec<(K, PyResult<Py<V>>)>,
    {
        {
            let read_guard = self.state.read_py_attached(py).unwrap();
            if let CacheState::Frozen { view, .. } = &*read_guard {
                return Ok(view.clone_ref(py).into_bound(py));
            }
        }

        // Avoid work inside write lock.
        // Tradeoff wasted work for lock contention.
        let entries: Vec<(K, Py<V>)> = f()
            .into_iter()
            .map(|(k, v_res)| Ok((k, v_res?)))
            .collect::<PyResult<Vec<_>>>()?;

        let mut write_guard = self.state.write_py_attached(py).unwrap();
        match &mut *write_guard {
            CacheState::Frozen { view, .. } => Ok(view.clone_ref(py).into_bound(py)),
            CacheState::Active { map } => {
                // This is relatively cheap to clone.
                // Enables propagating error from dict inserts.
                let mut frozen_map = map.clone();

                for (k, v) in entries {
                    frozen_map.entry(k).or_insert_with(|| v);
                }
                let dict = PyDict::new(py);
                for (k, v) in frozen_map.iter() {
                    dict.set_item(k.clone(), v.clone_ref(py))?;
                }

                let mapping = dict.as_mapping();
                let view = PyMappingProxy::new(py, mapping).unbind();

                let ret = view.clone_ref(py).into_bound(py);

                *write_guard = CacheState::Frozen {
                    map: frozen_map,
                    view,
                };

                Ok(ret)
            }
        }
    }
}

/// A Python module implemented in Rust.
#[pymodule]
#[pyo3(name = "_rust")]
fn scylla(py: Python<'_>, module: &Bound<'_, PyModule>) -> PyResult<()> {
    let _ = pyo3_log::try_init();
    add_submodule(
        py,
        module,
        "session_builder",
        session_builder::session_builder,
    )?;
    add_submodule(py, module, "session", session::session)?;
    add_submodule(py, module, "results", results::results)?;
    add_submodule(py, module, "statement", statement::statement)?;
    add_submodule(py, module, "enums", enums::enums)?;
    add_submodule(py, module, "errors", errors::errors)?;
    add_submodule(
        py,
        module,
        "execution_profile",
        execution_profile::execution_profile,
    )?;
    add_submodule(py, module, "types", types::types)?;
    add_submodule(py, module, "value", value::value)?;
    add_submodule(py, module, "batch", batch::batch)?;
    add_submodule(py, module, "policies", policies::policies)?;
    Ok(())
}
