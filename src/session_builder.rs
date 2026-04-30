use crate::RUNTIME;
use crate::errors::{DriverSessionConfigError, DriverSessionConnectionError};
use crate::execution_profile::ExecutionProfile;
use crate::policies::{
    AddressTranslatorInput, HostFilterInput, InternalAuthenticatorProvider,
    PyAuthenticatorProvider, TimestampGeneratorInput,
};
use crate::session::Session;
use pyo3::prelude::*;
use pyo3::types::PySequence;
use scylla::authentication::PlainTextAuthenticator;
use scylla::client::session::SessionConfig;
use std::net::{IpAddr, SocketAddr, ToSocketAddrs};
use std::str::FromStr;
use std::sync::Arc;
use std::time::Duration;

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
        contact_points: ContactPoints,
    ) -> PyRefMut<'py, Self> {
        contact_points.add_known_nodes(&mut slf.config);
        slf
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
        authenticator: Py<PyAuthenticatorProvider>,
    ) -> PyRefMut<'py, Self> {
        slf.config.authenticator = Some(Arc::new(InternalAuthenticatorProvider {
            python_authenticator: authenticator,
        }));

        slf
    }

    fn address_translator<'py>(
        mut slf: PyRefMut<'py, Self>,
        translator: AddressTranslatorInput,
    ) -> PyRefMut<'py, Self> {
        slf.config.address_translator = Some(translator.into_inner());

        slf
    }

    fn timestamp_generator<'py>(
        mut slf: PyRefMut<'py, Self>,
        generator: TimestampGeneratorInput,
    ) -> PyRefMut<'py, Self> {
        slf.config.timestamp_generator = Some(generator.into_inner());

        slf
    }
    fn host_filter<'py>(
        mut slf: PyRefMut<'py, Self>,
        host_filter: HostFilterInput,
    ) -> PyRefMut<'py, Self> {
        slf.config.host_filter = Some(host_filter.into_inner());

        slf
    }

    async fn connect(&self) -> Result<Session, DriverSessionConnectionError> {
        let config = self.config.clone();
        let session_result = RUNTIME
            .spawn(async move { scylla::client::session::Session::connect(config).await })
            .await?;
        match session_result {
            Ok(session) => Ok(Session {
                _inner: Arc::new(session),
            }),
            Err(err) => Err(DriverSessionConnectionError::new_session_error(err)),
        }
    }
}

pub(crate) enum ContactPoint {
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

impl ToSocketAddrs for ContactPoint {
    type Iter = std::vec::IntoIter<SocketAddr>;

    fn to_socket_addrs(&self) -> std::io::Result<Self::Iter> {
        match self {
            ContactPoint::SocketAddr(addr) => Ok(vec![*addr].into_iter()),
            ContactPoint::Host(host) => host.to_socket_addrs(),
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

impl TryFrom<ContactPoint> for SocketAddr {
    type Error = DriverSessionConfigError;

    fn try_from(value: ContactPoint) -> Result<Self, Self::Error> {
        match value {
            ContactPoint::SocketAddr(addr) => Ok(addr),
            ContactPoint::Host(addr_str) => SocketAddr::from_str(&addr_str).map_err(|reason| {
                DriverSessionConfigError::invalid_contact_point_addr(addr_str, reason)
            }),
        }
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

pub(crate) enum ContactPoints {
    Single(ContactPoint),
    Multiple(Vec<ContactPoint>),
}

impl IntoIterator for ContactPoints {
    type Item = ContactPoint;
    type IntoIter = std::vec::IntoIter<ContactPoint>;

    fn into_iter(self) -> Self::IntoIter {
        match self {
            ContactPoints::Single(cp) => vec![cp].into_iter(),
            ContactPoints::Multiple(cps) => cps.into_iter(),
        }
    }
}

impl ContactPoints {
    fn add_known_nodes(self, config: &mut SessionConfig) {
        match self {
            ContactPoints::Single(cp) => cp.add_known_node(config),
            ContactPoints::Multiple(seq) => {
                for cp in seq.into_iter() {
                    cp.add_known_node(config);
                }
            }
        }
    }
}

impl<'py> FromPyObject<'_, 'py> for ContactPoints {
    type Error = DriverSessionConfigError;

    fn extract(obj: Borrowed<'_, 'py, PyAny>) -> Result<Self, Self::Error> {
        if let Ok(s) = obj.extract::<ContactPoint>() {
            return Ok(ContactPoints::Single(s));
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

            return Ok(ContactPoints::Multiple(list));
        }

        Err(DriverSessionConfigError::contact_point_type_error(obj))
    }
}

#[pymodule]
pub(crate) fn session_builder(_py: Python<'_>, module: &Bound<'_, PyModule>) -> PyResult<()> {
    module.add_class::<SessionBuilder>()?;
    Ok(())
}
