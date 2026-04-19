use crate::RUNTIME;
use crate::enums::PyCompression;
use crate::errors::{DriverSessionConfigError, DriverSessionConnectionError};
use crate::execution_profile::ExecutionProfile;
use crate::policies::{
    InternalAddressTranslator, InternalAuthenticatorProvider, InternalHostFilter,
    InternalTimestampGenerator, PyAddressTranslator, PyAuthenticatorProvider, PyHostFilter,
    PyTimestampGenerator,
};
use crate::session::PySession;
use pyo3::prelude::*;
use pyo3::sync::MutexExt;
use pyo3::types::{PySequence, PyString};
use scylla::authentication::PlainTextAuthenticator;
use scylla::client::session::SessionConfig;
use scylla::routing::ShardAwarePortRange;
use std::convert::Infallible;
use std::net::{IpAddr, SocketAddr};
use std::ops::RangeInclusive;
use std::str::FromStr;
use std::str::FromStr;
use std::sync::{Arc, Mutex};
use std::time::Duration;

#[pyclass(frozen)]
struct SessionBuilder {
    inner: Mutex<PySessionBuilderConfig>,
}

#[pymethods]
impl SessionBuilder {
    #[new]
    fn new(py: Python) -> PyResult<Self> {
        Ok(Self {
            inner: Mutex::new(PySessionBuilderConfig::new(py)?),
        })
    }
    fn contact_points<'py>(
        slf: PyRef<'py, Self>,
        py: Python<'py>,
        contact_points: ContactPoints,
    ) -> PyRef<'py, Self> {
        {
            let mut inner = slf.inner.lock_py_attached(py).unwrap();

            contact_points.clone().add_known_nodes(&mut inner.config);
            inner.contact_points.extend(contact_points.inner);
        }

        slf
    }

    fn execution_profile<'py>(
        slf: PyRef<'py, Self>,
        py: Python<'py>,
        execution_profile: Py<ExecutionProfile>,
    ) -> PyRef<'py, Self> {
        {
            let mut inner = slf.inner.lock_py_attached(py).unwrap();
            inner.execution_profile = execution_profile.clone();
            inner.config.default_execution_profile_handle =
                execution_profile.get()._inner.clone().into_handle();
        }
        slf
    }

    fn user<'py>(
        slf: PyRef<'py, Self>,
        py: Python<'py>,
        username: String,
        password: String,
    ) -> PyRef<'py, Self> {
        {
            let mut inner = slf.inner.lock_py_attached(py).unwrap();
            inner.config.authenticator =
                Some(Arc::new(PlainTextAuthenticator::new(username, password)));
        }
        slf
    }

    fn authenticator_provider<'py>(
        slf: PyRef<'py, Self>,
        py: Python<'py>,
        authenticator: Py<PyAuthenticatorProvider>,
    ) -> PyRef<'py, Self> {
        {
            let mut inner = slf.inner.lock_py_attached(py).unwrap();
            inner.authenticator = Some(authenticator.clone().into());
            inner.config.authenticator = Some(Arc::new(InternalAuthenticatorProvider {
                python_authenticator: authenticator,
            }));
        }

        slf
    }

    fn address_translator<'py>(
        slf: PyRef<'py, Self>,
        py: Python<'py>,
        translator: Py<PyAddressTranslator>,
    ) -> PyRef<'py, Self> {
        {
            let mut inner = slf.inner.lock_py_attached(py).unwrap();
            inner.address_translator = Some(translator.clone().into());
            inner.config.address_translator = Some(Arc::new(InternalAddressTranslator {
                python_translator: translator,
            }));
        }

        slf
    }

    fn timestamp_generator<'py>(
        slf: PyRef<'py, Self>,
        py: Python<'py>,
        generator: Py<PyTimestampGenerator>,
    ) -> PyRef<'py, Self> {
        {
            let mut inner = slf.inner.lock_py_attached(py).unwrap();
            inner.timestamp_generator = Some(generator.clone().into());
            inner.config.timestamp_generator = Some(Arc::new(InternalTimestampGenerator {
                py_timestamp_generator: generator,
            }));
        }

        slf
    }

    fn host_filter<'py>(
        slf: PyRef<'py, Self>,
        py: Python<'py>,
        host_filter: Py<PyHostFilter>,
    ) -> PyRef<'py, Self> {
        {
            let mut inner = slf.inner.lock_py_attached(py).unwrap();
            inner.host_filter = Some(host_filter.clone().into());
            inner.config.host_filter = Some(Arc::new(InternalHostFilter {
                py_host_filter: host_filter,
            }));
        }

        slf
    }

    fn local_ip_address<'py>(
        slf: PyRef<'py, Self>,
        py: Python<'py>,
        ip: Option<IpAddr>,
    ) -> PyRef<'py, Self> {
        {
            let mut inner = slf.inner.lock_py_attached(py).unwrap();
            inner.config.local_ip_address = ip;
        }
        slf
    }

    fn shard_aware_local_port_range<'py>(
        slf: PyRef<'py, Self>,
        py: Python<'py>,
        port_range: (u16, u16),
    ) -> Result<PyRef<'py, Self>, DriverSessionConfigError> {
        {
            let mut inner = slf.inner.lock_py_attached(py).unwrap();
            inner.shard_aware_local_port_range = port_range;
            inner.config.shard_aware_local_port_range =
                ShardAwarePortRange::new(RangeInclusive::new(port_range.0, port_range.1))
                    .map_err(|_| DriverSessionConfigError::InvalidPortRange)?;
        }
        Ok(slf)
    }

    fn compression<'py>(
        slf: PyRef<'py, Self>,
        py: Python<'py>,
        compression: Option<PyCompression>,
    ) -> PyRef<'py, Self> {
        {
            let mut inner = slf.inner.lock_py_attached(py).unwrap();
            inner.config.compression = compression.map(|c| c.into());
        }
        slf
    }

    fn schema_agreement_interval<'py>(
        slf: PyRef<'py, Self>,
        py: Python<'py>,
        timeout: PyDuration,
    ) -> PyRef<'py, Self> {
        {
            let mut inner = slf.inner.lock_py_attached(py).unwrap();
            inner.config.schema_agreement_interval = timeout.0;
        }

        slf
    }

    pub fn tcp_nodelay<'py>(
        slf: PyRef<'py, Self>,
        py: Python<'py>,
        nodelay: bool,
    ) -> PyRef<'py, Self> {
        {
            let mut inner = slf.inner.lock_py_attached(py).unwrap();
            inner.config.tcp_nodelay = nodelay;
        }
        slf
    }

    fn tcp_keepalive_interval<'py>(
        slf: PyRef<'py, Self>,
        py: Python<'py>,
        interval: Option<PyDuration>,
    ) -> PyRef<'py, Self> {
        if let Some(ref dur) = interval
            && dur.0 <= Duration::from_secs(1)
        {
            log::warn!(
                "Setting the TCP keepalive interval to low values ({:?}) is not recommended as it can have a negative impact on performance. Consider setting it above 1 second.",
                dur.0
            );
        }

        {
            let mut inner = slf.inner.lock_py_attached(py).unwrap();
            inner.config.tcp_keepalive_interval = interval.map(|delta| delta.0);
        }

        slf
    }

    fn use_keyspace<'py>(
        slf: PyRef<'py, Self>,
        py: Python<'py>,
        keyspace_name: String,
        case_sensitive: bool,
    ) -> PyRef<'py, Self> {
        {
            let mut inner = slf.inner.lock_py_attached(py).unwrap();
            inner.config.used_keyspace = Some(keyspace_name);
            inner.config.keyspace_case_sensitive = case_sensitive;
        }
        slf
    }

    fn connection_timeout<'py>(
        slf: PyRef<'py, Self>,
        py: Python<'py>,
        timeout: PyDuration,
    ) -> PyRef<'py, Self> {
        {
            let mut inner = slf.inner.lock_py_attached(py).unwrap();
            inner.config.connect_timeout = timeout.0;
        }
        slf
    }
    async fn connect(&self) -> Result<PySession, DriverSessionConnectionError> {
        let config = Python::attach(|py| {
            let inner = self.inner.lock_py_attached(py).unwrap();
            inner.config.clone()
        });

        let session_result = RUNTIME
            .spawn(async move { scylla::client::session::Session::connect(config).await })
            .await?;
        match session_result {
            Ok(session) => PySession::try_from(Arc::new(session))
                .map_err(DriverSessionConnectionError::python_conversion_error),
            Err(err) => Err(DriverSessionConnectionError::new_session_error(err)),
        }
    }
}

#[derive(Clone)]
#[pyclass(name = "SessionBuilderConfig", frozen, skip_from_py_object)]
struct PySessionBuilderConfig {
    config: SessionConfig,
    #[pyo3(get)]
    pub execution_profile: Py<ExecutionProfile>,
    #[pyo3(get)]
    pub contact_points: Vec<ContactPoint>,
    #[pyo3(get)]
    pub host_filter: Option<Py<PyAny>>,
    #[pyo3(get)]
    pub authenticator: Option<Py<PyAny>>,
    #[pyo3(get)]
    pub address_translator: Option<Py<PyAny>>,
    #[pyo3(get)]
    pub timestamp_generator: Option<Py<PyAny>>,
    #[pyo3(get)]
    pub shard_aware_local_port_range: (u16, u16),
}

impl PySessionBuilderConfig {
    fn new(py: Python) -> PyResult<Self> {
        let config = SessionConfig::new();

        let execution_profile = Py::new(
            py,
            ExecutionProfile {
                _inner: config.default_execution_profile_handle.to_profile(),
            },
        )?;

        Ok(Self {
            config,
            execution_profile,
            shard_aware_local_port_range: (49152, 65535),
            contact_points: Vec::new(),
            host_filter: None,
            authenticator: None,
            address_translator: None,
            timestamp_generator: None,
        })
    }
}

pub(crate) struct PyDuration(pub(crate) Duration);

impl<'py> FromPyObject<'_, 'py> for PyDuration {
    type Error = DriverSessionConfigError;
    fn extract(obj: Borrowed<'_, 'py, PyAny>) -> Result<Self, Self::Error> {
        if let Ok(duration) = obj.extract::<Duration>() {
            return Ok(PyDuration(duration));
        }

        if let Ok(secs) = obj.extract::<f64>() {
            let duration = Duration::try_from_secs_f64(secs)
                .map_err(|_| DriverSessionConfigError::invalid_duration(obj))?;
            return Ok(PyDuration(duration));
        }

        Err(DriverSessionConfigError::invalid_duration(obj))
    }
}

#[derive(Clone)]
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

impl<'py> IntoPyObject<'py> for ContactPoint {
    type Target = PyString;
    type Output = Bound<'py, PyString>;
    type Error = Infallible;

    fn into_pyobject(self, py: Python<'py>) -> Result<Self::Output, Self::Error> {
        match self {
            ContactPoint::Host(host) => Ok(PyString::new(py, &host)),
            ContactPoint::SocketAddr(addr) => Ok(PyString::new(py, &addr.to_string())),
        }
    }
}

impl<'py> FromPyObject<'_, 'py> for ContactPoint {
    type Error = DriverSessionConfigError;

    fn extract(obj: Borrowed<'_, 'py, PyAny>) -> Result<Self, Self::Error> {
        if let Ok(s) = obj.extract::<String>() {
            return Ok(ContactPoint::Host(s));
        }

        if let Ok((host_str, port)) = obj.extract::<(&str, u16)>() {
            return if let Ok(ip) = IpAddr::from_str(host_str) {
                Ok(ContactPoint::SocketAddr(SocketAddr::new(ip, port)))
            } else {
                Ok(ContactPoint::Host(format!("{}:{}", host_str, port)))
            };
        }

        if let Ok((host, port)) = obj.extract::<(IpAddr, u16)>() {
            return Ok(ContactPoint::SocketAddr(SocketAddr::new(host, port)));
        }

        Err(DriverSessionConfigError::contact_point_type_error(obj))
    }
}

#[derive(Clone, Default)]
struct ContactPoints {
    inner: Vec<ContactPoint>,
}

impl ContactPoints {
    fn add_known_nodes(self, config: &mut SessionConfig) {
        for cp in self.inner.into_iter() {
            cp.add_known_node(config);
        }
    }
}

impl<'py> FromPyObject<'_, 'py> for ContactPoints {
    type Error = DriverSessionConfigError;

    fn extract(obj: Borrowed<'_, 'py, PyAny>) -> Result<Self, Self::Error> {
        if let Ok(s) = obj.extract::<ContactPoint>() {
            return Ok(ContactPoints { inner: vec![s] });
        }

        if let Ok(seq) = obj.cast::<PySequence>() {
            let iter = seq
                .try_iter()
                .map_err(DriverSessionConfigError::contact_points_iteration_failed)?;

            let list: Vec<ContactPoint> = iter
                .enumerate()
                .map(|(index, item_result)| {
                    let item = item_result.map_err(|e| {
                        DriverSessionConfigError::contact_points_invalid_item(index, e)
                    })?;

                    item.extract::<ContactPoint>().map_err(|e| {
                        DriverSessionConfigError::contact_points_invalid_item(index, e.into())
                    })
                })
                .collect::<Result<Vec<_>, _>>()?;

            return Ok(ContactPoints { inner: list });
        }

        Err(DriverSessionConfigError::contact_point_type_error(obj))
    }
}

#[pymodule]
pub(crate) fn session_builder(_py: Python<'_>, module: &Bound<'_, PyModule>) -> PyResult<()> {
    module.add_class::<SessionBuilder>()?;
    module.add_class::<PySessionBuilderConfig>()?;
    Ok(())
}
