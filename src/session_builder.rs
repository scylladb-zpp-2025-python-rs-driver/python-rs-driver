use crate::RUNTIME;
use crate::policies::{
    PyAddressTranslator, PyTimestampGenerator, InternalAuthenticator, InternalAddressTranslator,
    PyAuthenticator, InternalTimestampGenerator,
};
use crate::execution_profile::ExecutionProfile;
use crate::session::Session;
use pyo3::exceptions::{PyRuntimeError, PyTypeError};
use pyo3::prelude::*;
use pyo3::types::{PySequence, PyTuple};
use scylla::authentication::PlainTextAuthenticator;
use scylla::client::session::SessionConfig;
use std::net::{IpAddr, SocketAddr};
use std::str::FromStr;
use std::sync::Arc;

#[pyclass]
struct SessionBuilder {
    config: SessionConfig,
}

#[pymethods]
impl SessionBuilder {
    #[new]
    fn new() -> Self {
        Self {
            config: SessionConfig::new(),
        }
    }

    fn contact_points<'py>(
        mut slf: PyRefMut<'py, Self>,
        py: Python<'py>,
        contact_points: ContactPoints,
    ) -> PyResult<PyRefMut<'py, Self>> {
        contact_points.add_known_nodes(py, &mut slf.config)?;
        Ok(slf)
    }

    fn execution_profile<'py>(
        mut slf: PyRefMut<'py, Self>,
        execution_profile: ExecutionProfile,
    ) -> PyRefMut<'py, Self> {
        slf.config.default_execution_profile_handle = execution_profile._inner.into_handle();
        slf
    }

    fn user<'py>(
        mut slf: PyRefMut<'py, Self>,
        username: String,
        password: String,
    ) -> PyRefMut<'py, Self> {
        slf.config.authenticator = Some(Arc::new(PlainTextAuthenticator::new(username, password)));
        slf
    }

    fn authenticator_provider<'py>(
        mut slf: PyRefMut<'py, Self>,
        authenticator: Py<PyAuthenticator>,
    ) -> PyRefMut<'py, Self> {
        slf.config.authenticator = Some(Arc::new(InternalAuthenticator {
            python_authenticator: authenticator,
        }));

        slf
    }

    fn address_translator<'py>(
        mut slf: PyRefMut<'py, Self>,
        translator: Py<PyAddressTranslator>,
    ) -> PyRefMut<'py, Self> {
        slf.config.address_translator = Some(Arc::new(InternalAddressTranslator {
            python_translator: translator,
        }));

        slf
    }

    fn timestamp_generator<'py>(
        mut slf: PyRefMut<'py, Self>,
        generator: Py<PyTimestampGenerator>,
    ) -> PyRefMut<'py, Self> {
        slf.config.timestamp_generator = Some(Arc::new(InternalTimestampGenerator {
            py_timestamp_generator: generator,
        }));

        slf
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

enum ContactPoint {
    Host(String),
    SocketAddr(SocketAddr),
}

impl ContactPoint {
    fn add_known_node(self, config: &mut SessionConfig) {
        match self {
            ContactPoint::Host(host) => config.add_known_node(host),
            ContactPoint::SocketAddr(addr) => config.add_known_node_addr(addr),
        }
    }
}

impl<'py> FromPyObject<'_, 'py> for ContactPoint {
    type Error = PyErr;

    fn extract(obj: Borrowed<'_, 'py, PyAny>) -> Result<Self, Self::Error> {
        if let Ok(s) = obj.extract::<String>() {
            return Ok(ContactPoint::Host(s));
        }

        if let Ok(tuple) = obj.cast::<PyTuple>() {
            if tuple.len() != 2 {
                return Err(PyErr::new::<PyTypeError, _>(format!(
                    "Invalid tuple length: expected 2, got {}",
                    tuple.len()
                )));
            }

            let port = tuple.get_item(1)?.extract::<u16>().map_err(|_| {
                PyErr::new::<PyTypeError, _>("Port must be an integer in range 0-65535")
            })?;

            let host = tuple.get_item(0)?;

            if let Ok(host_str) = host.extract::<&str>() {
                // We attempt to parse the host as an IpAddr first to avoid incorrect formatting,
                // for IPv6 addresses
                return if let Ok(ip) = IpAddr::from_str(host_str) {
                    Ok(ContactPoint::SocketAddr(SocketAddr::new(ip, port)))
                } else {
                    Ok(ContactPoint::Host(format!("{}:{}", host_str, port)))
                };
            }

            if let Ok(host) = host.extract::<IpAddr>() {
                return Ok(ContactPoint::SocketAddr(SocketAddr::new(host, port)));
            }
        }

        Err(PyErr::new::<PyTypeError, _>(format!(
            "Invalid contact point type: expected str | tuple(str, int) | tuple(ipaddress, int), got {}",
            obj.get_type().name()?
        )))
    }
}

enum ContactPoints {
    Single(ContactPoint),
    Multiple(Py<PySequence>),
}

impl ContactPoints {
    fn add_known_nodes(self, py: Python, config: &mut SessionConfig) -> PyResult<()> {
        match self {
            ContactPoints::Single(cp) => cp.add_known_node(config),
            ContactPoints::Multiple(seq) => {
                let seq = seq.bind(py);
                for item in seq.try_iter()? {
                    let item = item?;
                    item.extract::<ContactPoint>()?.add_known_node(config);
                }
            }
        }

        Ok(())
    }
}

impl<'py> FromPyObject<'_, 'py> for ContactPoints {
    type Error = PyErr;

    fn extract(obj: Borrowed<'_, 'py, PyAny>) -> Result<Self, Self::Error> {
        if let Ok(s) = obj.extract::<ContactPoint>() {
            return Ok(ContactPoints::Single(s));
        }

        if let Ok(l) = obj.cast::<PySequence>() {
            return Ok(ContactPoints::Multiple(l.to_owned().unbind()));
        }

        Err(PyErr::new::<PyTypeError, _>(format!(
            "Invalid contact points type: expected str, tuple(host, port), or sequence of those, got {}",
            obj.get_type().name()?
        )))
    }
}

#[pymodule]
pub(crate) fn session_builder(_py: Python<'_>, module: &Bound<'_, PyModule>) -> PyResult<()> {
    module.add_class::<SessionBuilder>()?;
    Ok(())
}
