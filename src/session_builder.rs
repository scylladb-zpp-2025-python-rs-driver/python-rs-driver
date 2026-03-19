use std::sync::Arc;

use pyo3::prelude::*;
use pyo3::types::{PyInt, PySequence, PyString};
use scylla::client::session::SessionConfig;

use crate::RUNTIME;
use crate::errors::{ConnectionError, SessionConfigError};
use crate::execution_profile::ExecutionProfile;
use crate::session::Session;

#[pyclass]
struct SessionBuilder {
    config: SessionConfig,
}

#[pymethods]
impl SessionBuilder {
    #[new]
    #[pyo3(signature = (contact_points, port, execution_profile=None))]
    fn new(
        contact_points: Bound<'_, PySequence>,
        port: Bound<'_, PyInt>,
        execution_profile: Option<ExecutionProfile>,
    ) -> Result<Self, SessionConfigError> {
        let mut cfg = SessionConfig::new();

        let port = port
            .extract::<u16>()
            .map_err(SessionConfigError::invalid_port)?;

        if contact_points.is_instance_of::<PyString>() {
            return Err(SessionConfigError::contact_points_type_error());
        }

        let contact_points_iter = contact_points
            .try_iter()
            .map_err(SessionConfigError::contact_points_not_iterable)?;

        for (i, item_result) in contact_points_iter.enumerate() {
            let item = match item_result {
                Ok(item) => item,
                Err(err) => return Err(SessionConfigError::contact_point_access_failed(i, err)),
            };

            let s = item
                .cast_into::<PyString>()
                .map_err(|err| SessionConfigError::contact_point_type_error(i, err.into()))?;

            let s = s
                .to_str()
                .map_err(|err| SessionConfigError::contact_point_conversion_failed(i, err))?;
            if s.contains(":") {
                cfg.add_known_node(s);
            } else {
                cfg.add_known_node(format!("{}:{}", s, port));
            }
        }

        if let Some(execution_profile) = execution_profile {
            cfg.default_execution_profile_handle = execution_profile._inner.into_handle();
        }

        Ok(Self { config: cfg })
    }

    async fn connect(&self) -> Result<Session, ConnectionError> {
        let config = self.config.clone();
        let session_result = RUNTIME
            .spawn(async move { scylla::client::session::Session::connect(config).await })
            .await?;
        match session_result {
            Ok(session) => Ok(Session {
                _inner: Arc::new(session),
            }),
            Err(err) => Err(ConnectionError::new_session_error(err)),
        }
    }
}

#[pymodule]
pub(crate) fn session_builder(_py: Python<'_>, module: &Bound<'_, PyModule>) -> PyResult<()> {
    module.add_class::<SessionBuilder>()?;
    Ok(())
}
