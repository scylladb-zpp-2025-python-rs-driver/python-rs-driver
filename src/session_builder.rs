use std::sync::Arc;

use pyo3::exceptions::PyRuntimeError;
use pyo3::prelude::*;
use pyo3::types::{PyInt, PySequence, PyString};
use scylla::client::session::SessionConfig;

use crate::RUNTIME;
use crate::session::Session;

#[pyclass]
struct SessionBuilder {
    config: SessionConfig,
}

#[pymethods]
impl SessionBuilder {
    #[new]
    fn new(contact_points: Bound<'_, PySequence>, port: Bound<'_, PyInt>) -> PyResult<Self> {
        let mut cfg = SessionConfig::new();

        let port = port.extract::<u16>()?;

        if contact_points.is_instance_of::<PyString>() {
            return Err(PyRuntimeError::new_err(
                "contact_points should be a list of strings, not a string!",
            ));
        }

        for i in 0..contact_points.len().unwrap() {
            let item = contact_points
                .get_item(i)
                .unwrap()
                .downcast_into::<PyString>()?;
            let s = item.to_str()?;
            if s.contains(":") {
                cfg.add_known_node(s);
            } else {
                cfg.add_known_node(format!("{}:{}", s, port));
            }
        }

        Ok(Self { config: cfg })
    }

    async fn connect(&self) -> PyResult<Session> {
        let config = self.config.clone();
        let session_result = RUNTIME
            .spawn(async move { scylla::client::session::Session::connect(config).await })
            .await
            .expect("Driver should not panic");
        match session_result {
            Ok(session) => Ok(Session {
                _inner: Arc::new(session),
            }),
            Err(e) => Err(PyRuntimeError::new_err(format!(
                "Session creation err, e: {:?}, cp: {:?}",
                e, self.config.known_nodes
            ))),
        }
    }
}

#[pymodule]
pub(crate) fn session_builder(_py: Python<'_>, module: &Bound<'_, PyModule>) -> PyResult<()> {
    module.add_class::<SessionBuilder>()?;
    Ok(())
}
