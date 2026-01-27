use std::sync::Arc;

use pyo3::prelude::*;
use pyo3::types::{PyInt, PySequence, PyString};
use scylla::client::session::SessionConfig;

use crate::RUNTIME;
use crate::errors::{DriverExecutionError, ExecutionOp, ExecutionSource};
use crate::execution_profile::ExecutionProfile;
use crate::session::Session;

#[pyclass]
struct SessionBuilder {
    config: SessionConfig,
    // Keep for error reporting
    contact_points: Vec<String>,
    port: u16,
}

#[pymethods]
impl SessionBuilder {
    #[new]
    #[pyo3(signature = (contact_points, port, execution_profile=None))]
    fn new(
        contact_points: Bound<'_, PySequence>,
        port: Bound<'_, PyInt>,
        execution_profile: Option<ExecutionProfile>,
    ) -> Result<Self, DriverExecutionError> {
        let mut cfg = SessionConfig::new();

        let port = port.extract::<u16>().map_err(|e| {
            DriverExecutionError::bad_query(
                ExecutionOp::BuildSessionConfig,
                Some(crate::errors::ExecutionSource::PyErr(e)),
                "port must be an integer in range 0..65535",
            )
        })?;

        if contact_points.is_instance_of::<PyString>() {
            return Err(DriverExecutionError::bad_query(
                ExecutionOp::ParseContactPoints,
                None,
                "contact_points must be a sequence of strings, not a single string",
            ));
        }

        let mut contact_points_vec: Vec<String> = Vec::new();

        for i in 0..contact_points.len().unwrap() {
            // Get item i
            let item = contact_points.get_item(i).map_err(|e| {
                DriverExecutionError::bad_query(
                    ExecutionOp::BuildSessionConfig,
                    Some(crate::errors::ExecutionSource::PyErr(e)),
                    format!("failed to read contact_points[{i}]"),
                )
            })?;

            // Ensure it's a string
            let item = item.cast_into::<PyString>().map_err(|e| {
                // CastIntoError isn't a PyErr, but it implements Into<PyErr>
                let py_err: PyErr = e.into();
                DriverExecutionError::bad_query(
                    ExecutionOp::BuildSessionConfig,
                    Some(crate::errors::ExecutionSource::PyErr(py_err)),
                    format!("contact_points[{i}] must be a str"),
                )
            })?;

            // Convert to &str
            let s = item
                .to_str()
                .map_err(|e| {
                    DriverExecutionError::bad_query(
                        ExecutionOp::BuildSessionConfig,
                        Some(crate::errors::ExecutionSource::PyErr(e)),
                        format!("contact_points[{i}] must be valid UTF-8"),
                    )
                })?
                .to_string();

            contact_points_vec.push(s.clone());

            if s.contains(":") {
                cfg.add_known_node(s);
            } else {
                cfg.add_known_node(format!("{}:{}", s, port));
            }
        }

        if let Some(execution_profile) = execution_profile {
            cfg.default_execution_profile_handle = execution_profile._inner.into_handle();
        }

        Ok(Self {
            config: cfg,
            contact_points: contact_points_vec,
            port,
        })
    }

    async fn connect(&self) -> Result<Session, DriverExecutionError> {
        let config = self.config.clone();
        let contact_points = self.contact_points.clone();
        let port = self.port;

        let session_result = RUNTIME
            .spawn(async move { scylla::client::session::Session::connect(config).await })
            .await
            .map_err(|e| {
                DriverExecutionError::runtime(
                    ExecutionOp::SpawnJoin,
                    Some(ExecutionSource::RustErr(Box::new(e))),
                    "tokio join error while connecting",
                )
                .with_contact_points(contact_points.clone(), port)
            })?;

        match session_result {
            Ok(session) => Ok(Session {
                _inner: Arc::new(session),
            }),
            Err(e) => Err(DriverExecutionError::connect(
                ExecutionOp::Connect,
                Some(ExecutionSource::RustErr(Box::new(e))),
                "failed to connect to cluster",
            )
            .with_contact_points(contact_points, port)),
        }
    }
}

#[pymodule]
pub(crate) fn session_builder(_py: Python<'_>, module: &Bound<'_, PyModule>) -> PyResult<()> {
    module.add_class::<SessionBuilder>()?;
    Ok(())
}
