use crate::RUNTIME;
use crate::enums::{PyCompression, PyPoolSize};
use crate::errors::{DriverSessionConfigError, DriverSessionConnectionError};
use crate::execution_profile::ExecutionProfile;
use crate::policies::{
    InternalAddressTranslator, InternalAuthenticatorProvider, InternalHostFilter,
    InternalTimestampGenerator, PyAddressTranslator, PyAuthenticatorProvider, PyHostFilter,
    PyTimestampGenerator,
};
use crate::session::Session;
use pyo3::prelude::*;
use pyo3::types::PySequence;
use scylla::authentication::PlainTextAuthenticator;
use scylla::client::session::SessionConfig;
use scylla::routing::ShardAwarePortRange;
use std::net::{IpAddr, SocketAddr};
use std::ops::RangeInclusive;
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

    fn host_filter<'py>(
        mut slf: PyRefMut<'py, Self>,
        host_filter: Py<PyHostFilter>,
    ) -> PyRefMut<'py, Self> {
        slf.config.host_filter = Some(Arc::new(InternalHostFilter {
            py_host_filter: host_filter,
        }));

        slf
    }

    fn local_ip_address(mut slf: PyRefMut<'_, Self>, ip: Option<IpAddr>) -> PyRefMut<'_, Self> {
        slf.config.local_ip_address = ip;
        slf
    }

    fn shard_aware_local_port_range(
        mut slf: PyRefMut<'_, Self>,
        port_range: (u16, u16),
    ) -> Result<PyRefMut<'_, Self>, DriverSessionConfigError> {
        slf.config.shard_aware_local_port_range =
            ShardAwarePortRange::new(RangeInclusive::new(port_range.0, port_range.1))
                .map_err(|_| DriverSessionConfigError::InvalidPortRange)?;
        Ok(slf)
    }

    fn compression(
        mut slf: PyRefMut<'_, Self>,
        compression: Option<PyCompression>,
    ) -> PyRefMut<'_, Self> {
        slf.config.compression = compression.map(|c| c.into());
        slf
    }

    fn schema_agreement_interval<'py>(
        mut slf: PyRefMut<'py, Self>,
        timeout: PyDuration,
    ) -> PyRefMut<'py, Self> {
        slf.config.schema_agreement_interval = timeout.0;

        slf
    }

    pub fn tcp_nodelay(mut slf: PyRefMut<'_, Self>, nodelay: bool) -> PyRefMut<'_, Self> {
        slf.config.tcp_nodelay = nodelay;
        slf
    }

    fn tcp_keepalive_interval<'py>(
        mut slf: PyRefMut<'py, Self>,
        interval: Option<PyDuration>,
    ) -> PyRefMut<'py, Self> {
        if let Some(ref dur) = interval
            && dur.0 <= Duration::from_secs(1)
        {
            log::warn!(
                "Setting the TCP keepalive interval to low values ({:?}) is not recommended as it can have a negative impact on performance. Consider setting it above 1 second.",
                dur.0
            );
        }

        slf.config.tcp_keepalive_interval = interval.map(|delta| delta.0);

        slf
    }

    fn use_keyspace<'py>(
        mut slf: PyRefMut<'py, Self>,
        keyspace_name: String,
        case_sensitive: bool,
    ) -> PyRefMut<'py, Self> {
        slf.config.used_keyspace = Some(keyspace_name);
        slf.config.keyspace_case_sensitive = case_sensitive;
        slf
    }

    fn connection_timeout<'py>(
        mut slf: PyRefMut<'py, Self>,
        timeout: PyDuration,
    ) -> PyRefMut<'py, Self> {
        slf.config.connect_timeout = timeout.0;
        slf
    }

    fn pool_size<'py>(mut slf: PyRefMut<'py, Self>, size: PyPoolSize) -> PyRefMut<'py, Self> {
        slf.config.connection_pool_size = size.inner;
        slf
    }

    fn disallow_shard_aware_port(
        mut slf: PyRefMut<'_, Self>,
        disallow: bool,
    ) -> PyRefMut<'_, Self> {
        slf.config.disallow_shard_aware_port = disallow;
        slf
    }

    fn keyspaces_to_fetch(
        mut slf: PyRefMut<'_, Self>,
        keyspaces: Vec<String>,
    ) -> PyRefMut<'_, Self> {
        slf.config.keyspaces_to_fetch = keyspaces;
        slf
    }

    fn fetch_schema_metadata(mut slf: PyRefMut<'_, Self>, fetch: bool) -> PyRefMut<'_, Self> {
        slf.config.fetch_schema_metadata = fetch;
        slf
    }

    fn metadata_request_serverside_timeout(
        mut slf: PyRefMut<'_, Self>,
        timeout: PyDuration,
    ) -> PyRefMut<'_, Self> {
        slf.config.metadata_request_serverside_timeout = Some(timeout.0);
        slf
    }

    fn keepalive_interval(
        mut slf: PyRefMut<'_, Self>,
        interval: PyDuration,
    ) -> PyResult<PyRefMut<'_, Self>> {
        if interval.0 <= Duration::from_secs(1) {
            log::warn!(
                "Setting the keepalive interval to low values ({:?}) is not recommended as it can have a negative impact on performance. Consider setting it above 5 second.",
                interval.0
            );
        }

        slf.config.keepalive_interval = Some(interval.0);
        Ok(slf)
    }

    fn keepalive_timeout(mut slf: PyRefMut<'_, Self>, timeout: PyDuration) -> PyRefMut<'_, Self> {
        if timeout.0 <= Duration::from_secs(1) {
            log::warn!(
                "Setting the keepalive timeout to low values ({:?}) is not recommended as it can have a negative impact on performance. Consider setting it above 5 second.",
                timeout.0
            );
        }

        slf.config.keepalive_timeout = Some(timeout.0);
        slf
    }

    fn schema_agreement_timeout(
        mut slf: PyRefMut<'_, Self>,
        timeout: PyDuration,
    ) -> PyRefMut<'_, Self> {
        slf.config.schema_agreement_timeout = timeout.0;
        slf
    }

    fn auto_await_schema_agreement(
        mut slf: PyRefMut<'_, Self>,
        enabled: bool,
    ) -> PyRefMut<'_, Self> {
        slf.config.schema_agreement_automatic_waiting = enabled;
        slf
    }

    fn hostname_resolution_timeout(
        mut slf: PyRefMut<'_, Self>,
        duration: Option<PyDuration>,
    ) -> PyRefMut<'_, Self> {
        slf.config.hostname_resolution_timeout = duration.map(|d| d.0);
        slf
    }

    fn refresh_metadata_on_auto_schema_agreement(
        mut slf: PyRefMut<'_, Self>,
        refresh_metadata: bool,
    ) -> PyRefMut<'_, Self> {
        slf.config.refresh_metadata_on_auto_schema_agreement = refresh_metadata;
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

struct PyDuration(Duration);

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

enum ContactPoints {
    Single(ContactPoint),
    Multiple(Vec<ContactPoint>),
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
