#[cfg(test)]
mod tests {
    use pyo3::prelude::*;
    use pyo3::types::PyInt;
    use std::sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    };

    use crate::Cache;

    #[test]
    fn get_or_init_inserts_and_reuses() {
        Python::initialize();
        Python::attach(|py| {
            let cache = Cache::<String, PyInt>::new();
            let calls = Arc::new(AtomicUsize::new(0));

            let key = "a".to_string();

            let calls1 = calls.clone();
            let v1 = cache
                .get_or_init(py, &key, |_| {
                    calls1.fetch_add(1, Ordering::SeqCst);
                    Ok(Some(42i64.into_pyobject(py)?.unbind()))
                })
                .unwrap()
                .unwrap();

            let calls2 = calls.clone();
            let v2 = cache
                .get_or_init(py, &key, |_| {
                    calls2.fetch_add(1, Ordering::SeqCst);
                    Ok(Some(999i64.into_pyobject(py)?.unbind()))
                })
                .unwrap()
                .unwrap();

            assert_eq!(calls.load(Ordering::SeqCst), 1);

            let ptr1 = v1.bind(py).as_ptr();
            let ptr2 = v2.bind(py).as_ptr();
            assert!(ptr1.eq(&ptr2));
        });
    }

    #[test]
    fn get_or_init_none_does_not_insert() {
        Python::initialize();
        Python::attach(|py| {
            let cache = Cache::<String, PyInt>::new();
            let key = "missing".to_string();

            let r = cache.get_or_init(py, &key, |_| Ok(None)).unwrap();
            assert!(r.is_none());

            let r2 = cache.get_or_init(py, &key, |_| Ok(None)).unwrap();
            assert!(r2.is_none());
        });
    }

    #[test]
    fn freeze_blocks_new_keys_and_exposes_mapping() {
        Python::initialize();
        Python::attach(|py| {
            let cache = Cache::<String, PyInt>::new();

            // Pre-populate one key in active mode.
            let k1 = "x".to_string();
            cache
                .get_or_init(py, &k1, |_| Ok(Some(1i64.into_pyobject(py)?.unbind())))
                .unwrap();

            // Freeze with additional entries.
            let proxy = cache
                .get_or_init_python_mapping(py, || {
                    vec![
                        (
                            "x".to_string(),
                            Ok(111i64.into_pyobject(py).unwrap().unbind()),
                        ), // should not replace existing
                        (
                            "y".to_string(),
                            Ok(2i64.into_pyobject(py).unwrap().unbind()),
                        ),
                    ]
                })
                .unwrap();

            let x: i64 = proxy.get_item("x").unwrap().extract().unwrap();
            let y: i64 = proxy.get_item("y").unwrap().extract().unwrap();
            assert_eq!(x, 1);
            assert_eq!(y, 2);

            // After freeze, no new keys via get_or_init.
            let z = "z".to_string();
            let r = cache
                .get_or_init(py, &z, |_| Ok(Some(3i64.into_pyobject(py)?.unbind())))
                .unwrap();
            assert!(r.is_none());
        });
    }

    #[test]
    fn freeze_is_idempotent_for_state() {
        Python::initialize();
        Python::attach(|py| {
            let cache = Cache::<String, PyInt>::new();

            let p1 = cache
                .get_or_init_python_mapping(py, || {
                    vec![(
                        "a".to_string(),
                        Ok(10i64.into_pyobject(py).unwrap().unbind()),
                    )]
                })
                .unwrap();

            let p2 = cache
                .get_or_init_python_mapping(py, || {
                    vec![(
                        "b".to_string(),
                        Ok(20i64.into_pyobject(py).unwrap().unbind()),
                    )]
                })
                .unwrap();

            let a1: i64 = p1.get_item("a").unwrap().extract().unwrap();
            let a2: i64 = p2.get_item("a").unwrap().extract().unwrap();
            assert_eq!(a1, 10);
            assert_eq!(a2, 10);

            // "b" should not appear because second freeze sees already frozen state.
            assert!(p2.get_item("b").is_err());
        });
    }

    #[test]
    fn get_or_init_does_not_run_initializer_when_key_already_present() {
        Python::initialize();
        Python::attach(|py| {
            let cache = Cache::<String, PyInt>::new();
            let key = "present".to_string();

            cache
                .get_or_init(py, &key, |_| Ok(Some(7i64.into_pyobject(py)?.unbind())))
                .unwrap();

            let calls = Arc::new(AtomicUsize::new(0));
            let calls2 = calls.clone();

            let value = cache
                .get_or_init(py, &key, |_| {
                    calls2.fetch_add(1, Ordering::SeqCst);
                    Ok(Some(999i64.into_pyobject(py)?.unbind()))
                })
                .unwrap()
                .unwrap();

            let extracted: i64 = value.extract(py).unwrap();
            assert_eq!(extracted, 7);
            assert_eq!(calls.load(Ordering::SeqCst), 0);
        });
    }

    #[test]
    fn get_or_init_does_not_run_initializer_when_cache_is_frozen_and_key_missing() {
        Python::initialize();
        Python::attach(|py| {
            let cache = Cache::<String, PyInt>::new();

            cache
                .get_or_init_python_mapping(py, || {
                    vec![(
                        "a".to_string(),
                        Ok(1i64.into_pyobject(py).unwrap().unbind()),
                    )]
                })
                .unwrap();

            let missing = "missing_after_freeze".to_string();
            let calls = Arc::new(AtomicUsize::new(0));
            let calls2 = calls.clone();

            let result = cache
                .get_or_init(py, &missing, |_| {
                    calls2.fetch_add(1, Ordering::SeqCst);
                    Ok(Some(5i64.into_pyobject(py)?.unbind()))
                })
                .unwrap();

            assert!(result.is_none());
            assert_eq!(calls.load(Ordering::SeqCst), 0);
        });
    }

    #[test]
    fn get_or_init_supports_borrowed_str_lookup_for_string_keys() {
        Python::initialize();
        Python::attach(|py| {
            let cache = Cache::<String, PyInt>::new();

            let v1 = cache
                .get_or_init(py, "borrowed", |_| {
                    Ok(Some(11i64.into_pyobject(py)?.unbind()))
                })
                .unwrap()
                .unwrap();

            let v2 = cache
                .get_or_init(py, "borrowed", |_| {
                    Ok(Some(99i64.into_pyobject(py)?.unbind()))
                })
                .unwrap()
                .unwrap();

            let i1: i64 = v1.extract(py).unwrap();
            let i2: i64 = v2.extract(py).unwrap();
            assert_eq!(i1, 11);
            assert_eq!(i2, 11);
        });
    }

    #[test]
    fn get_or_init_mixes_string_insert_and_str_lookup() {
        Python::initialize();
        Python::attach(|py| {
            let cache = Cache::<String, PyInt>::new();
            let owned = "mixed".to_string();

            cache
                .get_or_init(py, &owned, |_| Ok(Some(21i64.into_pyobject(py)?.unbind())))
                .unwrap();

            let calls = Arc::new(AtomicUsize::new(0));
            let calls2 = calls.clone();

            let v = cache
                .get_or_init(py, "mixed", |_| {
                    calls2.fetch_add(1, Ordering::SeqCst);
                    Ok(Some(1000i64.into_pyobject(py)?.unbind()))
                })
                .unwrap()
                .unwrap();

            let extracted: i64 = v.extract(py).unwrap();
            assert_eq!(extracted, 21);
            assert_eq!(calls.load(Ordering::SeqCst), 0);
        });
    }
}
