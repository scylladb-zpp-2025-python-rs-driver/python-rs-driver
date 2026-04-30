use crate::errors::DriverSessionConfigError;
use crate::session_builder::{NodeAddr, NodeAddrs, PyDuration};
use async_trait::async_trait;
use pyo3::exceptions::PyNotImplementedError;
use pyo3::prelude::{PyAnyMethods, PyDictMethods, PyModule, PyModuleMethods};
use pyo3::types::{PyDict, PyString, PyTuple};
use pyo3::{
    Borrowed, Bound, BoundObject, FromPyObject, Py, PyAny, PyResult, Python, intern, pyclass,
    pymethods, pymodule,
};
use scylla::authentication::{AuthError, AuthenticatorProvider, AuthenticatorSession};
use scylla::cluster::metadata::Peer;
use scylla::errors::{CustomTranslationError, TranslationError};
use scylla::policies::address_translator::{AddressTranslator, UntranslatedPeer};
use scylla::policies::host_filter::{
    AcceptAllHostFilter, AllowListHostFilter, DcHostFilter, HostFilter,
};
use scylla::policies::timestamp_generator::{
    MonotonicTimestampGenerator, SimpleTimestampGenerator, TimestampGenerator,
};
use std::collections::HashMap;
use std::net::{IpAddr, SocketAddr};
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

#[derive(Clone)]
#[pyclass(subclass, skip_from_py_object, name = "AuthenticatorProvider")]
pub(crate) struct PyAuthenticatorProvider {}

#[pymethods]
impl PyAuthenticatorProvider {
    #[expect(unused_variables)]
    #[new]
    #[pyo3(signature = (*args, **kwargs))]
    pub fn new(args: &Bound<'_, PyTuple>, kwargs: Option<&Bound<'_, PyDict>>) -> Self {
        PyAuthenticatorProvider {}
    }

    fn new_authenticator(&self, _authenticator_name: &str) -> PyResult<Py<PyAuthenticator>> {
        Err(PyNotImplementedError::new_err("Method is not implemented"))
    }
}

#[derive(Clone)]
pub(crate) struct InternalAuthenticatorProvider {
    pub(crate) python_authenticator: Py<PyAuthenticatorProvider>,
}

#[async_trait]
impl AuthenticatorProvider for InternalAuthenticatorProvider {
    async fn start_authentication_session(
        &self,
        authenticator_name: &str,
    ) -> Result<(Option<Vec<u8>>, Box<dyn AuthenticatorSession>), AuthError> {
        let (result, py_auth) = Python::attach(
            |py| -> PyResult<(Option<Vec<u8>>, Box<InternalAuthenticator>)> {
                let py_auth_provider = self.python_authenticator.bind(py);

                let py_auth_any =
                    py_auth_provider.call_method1("new_authenticator", (authenticator_name,))?;

                let py_auth = py_auth_any.cast::<PyAuthenticator>()?;

                let response = py_auth
                    .call_method0("initial_response")?
                    .extract::<Option<Vec<u8>>>()?;

                Ok((
                    response,
                    Box::new(InternalAuthenticator {
                        python_authenticator: py_auth.to_owned().unbind(),
                    }),
                ))
            },
        )
        .map_err(|e| format!("Python new_authenticator failed: {:?}", e))?;

        Ok((result, py_auth))
    }
}

#[pyclass(subclass, name = "Authenticator")]
pub(crate) struct PyAuthenticator {}

#[pymethods]
impl PyAuthenticator {
    #[expect(unused_variables)]
    #[new]
    #[pyo3(signature = (*args, **kwargs))]
    pub fn new(args: &Bound<'_, PyTuple>, kwargs: Option<&Bound<'_, PyDict>>) -> Self {
        PyAuthenticator {}
    }

    fn initial_response(&self) -> PyResult<Option<Vec<u8>>> {
        Ok(None)
    }

    fn evaluate_challenge(&self, _challenge: Option<&[u8]>) -> PyResult<Option<Vec<u8>>> {
        Err(PyNotImplementedError::new_err("Method is not implemented"))
    }

    fn success(&self, _token: Option<&[u8]>) -> PyResult<()> {
        Ok(())
    }
}

pub(crate) struct InternalAuthenticator {
    pub(crate) python_authenticator: Py<PyAuthenticator>,
}

#[async_trait]
impl AuthenticatorSession for InternalAuthenticator {
    async fn evaluate_challenge(
        &mut self,
        token: Option<&[u8]>,
    ) -> Result<Option<Vec<u8>>, AuthError> {
        let result = Python::attach(|py| -> PyResult<Option<Vec<u8>>> {
            let py_auth = self.python_authenticator.bind(py);

            py_auth
                .call_method1("evaluate_challenge", (token,))?
                .extract::<Option<Vec<u8>>>()
        })
        .map_err(|e| format!("Python evaluate_challenge failed: {:?}", e))?;

        Ok(result)
    }

    async fn success(&mut self, token: Option<&[u8]>) -> Result<(), AuthError> {
        let result = Python::attach(|py| -> PyResult<()> {
            let py_auth = self.python_authenticator.bind(py);

            py_auth.call_method1("success", (token,))?;

            Ok(())
        })
        .map_err(|e| format!("Python success failed: {:?}", e))?;

        Ok(result)
    }
}

struct InternalAddressTranslator {
    inner: Py<PyAny>,
}

#[async_trait]
impl AddressTranslator for InternalAddressTranslator {
    async fn translate_address(
        &self,
        untranslated_peer: &UntranslatedPeer,
    ) -> Result<SocketAddr, TranslationError> {
        Python::attach(|py| -> PyResult<SocketAddr> {
            let py_trans = self.inner.bind(py);
            let peer_info = PyUntranslatedPeer::from(untranslated_peer);

            let translated = py_trans
                .call_method1(intern!(py, "translate"), (peer_info,))?
                .extract::<NodeAddr>()?;

            SocketAddr::try_from(translated).map_err(|e| e.into())
        })
        .map_err(|e| CustomTranslationError::new(e).into())
    }
}

pub(crate) struct AddressTranslatorInput {
    inner: Arc<dyn AddressTranslator>,
}

impl AddressTranslatorInput {
    pub(crate) fn into_inner(self) -> Arc<dyn AddressTranslator> {
        self.inner
    }
}

impl<'py> FromPyObject<'_, 'py> for AddressTranslatorInput {
    type Error = DriverSessionConfigError;

    fn extract(obj: Borrowed<'_, 'py, PyAny>) -> Result<Self, Self::Error> {
        if let Ok(dict) = obj.cast::<PyDict>() {
            let map = dict
                .iter()
                .enumerate()
                .map(|(idx, (k, v))| {
                    let from = k
                        .extract::<NodeAddr>()
                        .and_then(SocketAddr::try_from)
                        .map_err(|e| {
                            DriverSessionConfigError::invalid_node_addr_item(idx, e.into())
                        })?;

                    let to = v
                        .extract::<NodeAddr>()
                        .and_then(SocketAddr::try_from)
                        .map_err(|e| {
                            DriverSessionConfigError::invalid_node_addr_item(idx, e.into())
                        })?;

                    Ok((from, to))
                })
                .collect::<Result<HashMap<SocketAddr, SocketAddr>, Self::Error>>()?;

            return Ok(Self {
                inner: Arc::new(map),
            });
        }

        if !obj.hasattr(intern!(obj.py(), "translate")).unwrap_or(false) {
            return Err(DriverSessionConfigError::invalid_address_translator(obj));
        }

        Ok(Self {
            inner: Arc::new(InternalAddressTranslator {
                inner: obj.unbind(),
            }),
        })
    }
}

#[pyclass(get_all, name = "UntranslatedPeer", frozen)]
pub struct PyUntranslatedPeer {
    pub host_id: uuid::Uuid,
    pub untranslated_address: (IpAddr, u16),
    pub datacenter: Option<String>,
    pub rack: Option<String>,
}

#[pymethods]
impl PyUntranslatedPeer {
    fn __repr__(&self, py: Python<'_>) -> PyResult<Py<PyString>> {
        let (ip, port) = self.untranslated_address;

        let repr_str = PyString::from_fmt(
            py,
            format_args!(
                "UntranslatedPeer(host_id='{}', untranslated_address=('{}', {}), datacenter={:?}, rack={:?})",
                self.host_id, ip, port, self.datacenter, self.rack
            ),
        )?;

        Ok(repr_str.into())
    }
}

impl From<&UntranslatedPeer<'_>> for PyUntranslatedPeer {
    fn from(peer: &UntranslatedPeer) -> Self {
        Self {
            host_id: peer.host_id(),
            untranslated_address: (
                peer.untranslated_address().ip(),
                peer.untranslated_address().port(),
            ),
            datacenter: peer.datacenter().map(|s| s.to_string()),
            rack: peer.rack().map(|s| s.to_string()),
        }
    }
}

struct InternalTimestampGenerator {
    py_timestamp_generator: Py<PyAny>,
}
impl TimestampGenerator for InternalTimestampGenerator {
    fn next_timestamp(&self) -> i64 {
        Python::attach(|py| {
            let py_generator = self.py_timestamp_generator.bind(py);

            py_generator
                .call_method0(intern!(py, "next_timestamp"))
                .and_then(|res| res.extract::<i64>())
                .unwrap_or_else(|err| {
                    log::error!("Failed to generate custom timestamp from Python: {}", err);

                    // Returns current system time in microseconds as a fallback
                    SystemTime::now()
                        .duration_since(UNIX_EPOCH)
                        .map(|d| d.as_micros() as i64)
                        .unwrap_or(0)
                })
        })
    }
}

pub(crate) struct TimestampGeneratorInput {
    inner: Arc<dyn TimestampGenerator>,
}

impl TimestampGeneratorInput {
    pub(crate) fn into_inner(self) -> Arc<dyn TimestampGenerator> {
        self.inner
    }
}

impl<'py> FromPyObject<'_, 'py> for TimestampGeneratorInput {
    type Error = DriverSessionConfigError;

    fn extract(obj: Borrowed<'_, 'py, PyAny>) -> Result<Self, Self::Error> {
        if let Ok(monotonic) = obj.cast::<PyMonotonicTimestampGenerator>() {
            return Ok(Self {
                inner: monotonic.get().inner.clone(),
            });
        }

        if let Ok(simple) = obj.cast::<PySimpleTimestampGenerator>() {
            return Ok(Self {
                inner: simple.get().inner.clone(),
            });
        }

        if !obj
            .hasattr(intern!(obj.py(), "next_timestamp"))
            .unwrap_or(false)
        {
            return Err(DriverSessionConfigError::invalid_timestamp_generator(obj));
        }

        Ok(Self {
            inner: Arc::new(InternalTimestampGenerator {
                py_timestamp_generator: obj.unbind(),
            }),
        })
    }
}

#[pyclass(name = "MonotonicTimestampGenerator", frozen)]
struct PyMonotonicTimestampGenerator {
    inner: Arc<MonotonicTimestampGenerator>,
}

#[pymethods]
impl PyMonotonicTimestampGenerator {
    #[new]
    #[pyo3(signature = (warn_on_drift=true, warning_threshold=PyDuration(Duration::from_secs(1)), warning_interval=PyDuration(Duration::from_secs(1))))]
    pub fn new(
        warn_on_drift: bool,
        warning_threshold: PyDuration,
        warning_interval: PyDuration,
    ) -> Self {
        let mut monotonic_timestamp_generator = MonotonicTimestampGenerator::new()
            .with_warning_times(warning_threshold.0, warning_interval.0);

        if !warn_on_drift {
            monotonic_timestamp_generator = monotonic_timestamp_generator.without_warnings();
        }

        PyMonotonicTimestampGenerator {
            inner: Arc::new(monotonic_timestamp_generator),
        }
    }

    pub fn next_timestamp(&self) -> i64 {
        self.inner.next_timestamp()
    }
}

#[pyclass(name = "SimpleTimestampGenerator", frozen)]
struct PySimpleTimestampGenerator {
    inner: Arc<SimpleTimestampGenerator>,
}

#[pymethods]
impl PySimpleTimestampGenerator {
    #[new]
    pub fn new() -> Self {
        PySimpleTimestampGenerator {
            inner: Arc::new(SimpleTimestampGenerator {}),
        }
    }

    pub fn next_timestamp(&self) -> i64 {
        self.inner.next_timestamp()
    }
}

struct InternalHostFilter {
    py_host_filter: Py<PyAny>,
}
impl HostFilter for InternalHostFilter {
    fn accept(&self, peer: &Peer) -> bool {
        Python::attach(|py| {
            let py_filter = self.py_host_filter.bind(py);
            let py_peer = PyPeer::from(peer.clone());

            py_filter
                .call_method1(intern!(py, "accept"), (py_peer,))
                .and_then(|res| res.extract::<bool>())
                .unwrap_or_else(|err| {
                    log::error!("Failed to evaluate custom host filter from Python: {}", err);
                    true
                })
        })
    }
}

pub(crate) struct HostFilterInput {
    inner: Arc<dyn HostFilter>,
}

impl HostFilterInput {
    pub(crate) fn into_inner(self) -> Arc<dyn HostFilter> {
        self.inner
    }
}

impl<'py> FromPyObject<'_, 'py> for HostFilterInput {
    type Error = DriverSessionConfigError;

    fn extract(obj: Borrowed<'_, 'py, PyAny>) -> Result<Self, Self::Error> {
        if let Ok(points) = obj.extract::<NodeAddrs>() {
            let filter = AllowListHostFilter::new(points.inner)
                .map_err(|_| DriverSessionConfigError::InvalidHostFilterAddress)?;
            return Ok(Self {
                inner: Arc::new(filter),
            });
        }

        if let Ok(filter) = obj.cast::<PyAcceptAllHostFilter>() {
            return Ok(Self {
                inner: filter.get().inner.clone(),
            });
        }

        if let Ok(filter) = obj.cast::<PyDcHostFilter>() {
            return Ok(Self {
                inner: filter.get().inner.clone(),
            });
        }

        if !obj.hasattr(intern!(obj.py(), "accept")).unwrap_or(false) {
            return Err(DriverSessionConfigError::invalid_host_filter(obj));
        }

        Ok(Self {
            inner: Arc::new(InternalHostFilter {
                py_host_filter: obj.to_owned().unbind(),
            }),
        })
    }
}

#[pyclass(name = "AcceptAllHostFilter", frozen)]
struct PyAcceptAllHostFilter {
    inner: Arc<AcceptAllHostFilter>,
}

#[pymethods]
impl PyAcceptAllHostFilter {
    #[new]
    pub fn new() -> Self {
        PyAcceptAllHostFilter {
            inner: Arc::new(AcceptAllHostFilter {}),
        }
    }

    pub fn accept(&self, _peer: Py<PyPeer>) -> bool {
        true
    }
}

#[pyclass(name = "DcHostFilter", frozen)]
struct PyDcHostFilter {
    inner: Arc<DcHostFilter>,
}

#[pymethods]
impl PyDcHostFilter {
    #[new]
    pub fn new(local_dc: String) -> Self {
        PyDcHostFilter {
            inner: Arc::new(DcHostFilter::new(local_dc)),
        }
    }

    pub fn accept(&self, peer: Py<PyPeer>) -> bool {
        self.inner.accept(&peer.get().inner)
    }
}

#[pyclass(frozen, name = "Peer")]
pub struct PyPeer {
    inner: Peer,
    #[pyo3(get)]
    pub host_id: uuid::Uuid,
    #[pyo3(get)]
    pub address: (IpAddr, u16),
    //TODO when LBC is merged switch to using Token instead of i64
    #[pyo3(get)]
    pub tokens: Vec<i64>,
    #[pyo3(get)]
    pub datacenter: Option<String>,
    #[pyo3(get)]
    pub rack: Option<String>,
}

#[pymethods]
impl PyPeer {
    fn __repr__(&self, py: Python<'_>) -> PyResult<Py<PyString>> {
        let (ip, port) = self.address;

        let repr_str = PyString::from_fmt(
            py,
            format_args!(
                "Peer(host_id='{}', address=('{}', {}), tokens={:?}, datacenter={:?}, rack={:?})",
                self.host_id, ip, port, self.tokens, self.datacenter, self.rack
            ),
        )?;

        Ok(repr_str.into())
    }
}

impl From<Peer> for PyPeer {
    fn from(peer: Peer) -> Self {
        Self {
            host_id: peer.host_id,
            address: (peer.address.ip(), peer.address.port()),
            tokens: peer.tokens.iter().map(|t| t.value()).collect(),
            datacenter: peer.datacenter.clone(),
            rack: peer.rack.clone(),
            inner: peer,
        }
    }
}

#[pymodule]
pub(crate) fn policies(_py: Python<'_>, module: &Bound<'_, PyModule>) -> PyResult<()> {
    module.add_class::<PyAuthenticatorProvider>()?;
    module.add_class::<PyAuthenticator>()?;
    module.add_class::<PyUntranslatedPeer>()?;
    module.add_class::<PyMonotonicTimestampGenerator>()?;
    module.add_class::<PySimpleTimestampGenerator>()?;
    module.add_class::<PyAcceptAllHostFilter>()?;
    module.add_class::<PyDcHostFilter>()?;
    module.add_class::<PyPeer>()?;
    Ok(())
}
