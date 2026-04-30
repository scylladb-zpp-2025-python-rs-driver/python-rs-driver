use crate::errors::DriverSessionConfigError;
use crate::session_builder::{NodeAddr, NodeAddrs, PyDuration};
use async_trait::async_trait;
use pyo3::exceptions::PyNotImplementedError;
use pyo3::prelude::{PyAnyMethods, PyDictMethods, PyModule, PyModuleMethods};
use pyo3::types::{PyDict, PyString, PyTuple};
use pyo3::{
    Borrowed, Bound, FromPyObject, Py, PyAny, PyClassInitializer, PyResult, Python, pyclass,
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

#[pyclass(subclass, skip_from_py_object, name = "AuthenticatorProvider", frozen)]
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

struct InternalAuthenticatorProvider {
    python_authenticator: Py<PyAuthenticatorProvider>,
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

#[pyclass(subclass, name = "Authenticator", frozen)]
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

struct InternalAuthenticator {
    python_authenticator: Py<PyAuthenticator>,
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

pub(crate) struct AuthenticatorProviderInput {
    inner: Arc<dyn AuthenticatorProvider>,
}

impl AuthenticatorProviderInput {
    pub(crate) fn into_inner(self) -> Arc<dyn AuthenticatorProvider> {
        self.inner
    }
}

impl<'py> FromPyObject<'_, 'py> for AuthenticatorProviderInput {
    type Error = DriverSessionConfigError;

    fn extract(obj: Borrowed<'_, 'py, PyAny>) -> Result<Self, Self::Error> {
        if let Ok(python_authenticator) = obj.extract::<Py<PyAuthenticatorProvider>>() {
            return Ok(Self {
                inner: Arc::new(InternalAuthenticatorProvider {
                    python_authenticator,
                }),
            });
        }

        Err(DriverSessionConfigError::invalid_authenticator_provider(
            obj,
        ))
    }
}

#[pyclass(subclass, skip_from_py_object, name = "AddressTranslator", frozen)]

pub(crate) struct PyAddressTranslator {}

#[pymethods]
impl PyAddressTranslator {
    #[expect(unused_variables)]
    #[new]
    #[pyo3(signature = (*args, **kwargs))]
    pub fn new(args: &Bound<'_, PyTuple>, kwargs: Option<&Bound<'_, PyDict>>) -> Self {
        PyAddressTranslator {}
    }

    fn translate(&self, _addr: Py<PyUntranslatedPeer>) -> PyResult<(IpAddr, u16)> {
        Err(PyNotImplementedError::new_err("Method is not implemented"))
    }
}
struct InternalAddressTranslator {
    inner: Py<PyAddressTranslator>,
}

#[async_trait]
impl AddressTranslator for InternalAddressTranslator {
    async fn translate_address(
        &self,
        untranslated_peer: &UntranslatedPeer,
    ) -> Result<SocketAddr, TranslationError> {
        let result = Python::attach(|py| -> PyResult<(IpAddr, u16)> {
            let py_trans = self.inner.bind(py);
            let py_peer_info = PyUntranslatedPeer::from(untranslated_peer);

            py_trans
                .call_method1("translate", (py_peer_info,))?
                .extract::<(IpAddr, u16)>()
        })
        .map_err(CustomTranslationError::new)?;

        Ok(SocketAddr::from(result))
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
        if let Ok(translator) = obj.extract::<Py<PyAddressTranslator>>() {
            return Ok(Self {
                inner: Arc::new(InternalAddressTranslator { inner: translator }),
            });
        }

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

        Err(DriverSessionConfigError::invalid_address_translator(obj))
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

#[pyclass(subclass, skip_from_py_object, name = "TimestampGenerator", frozen)]
pub(crate) struct PyTimestampGenerator {}

#[pymethods]
impl PyTimestampGenerator {
    #[expect(unused_variables)]
    #[new]
    #[pyo3(signature = (*args, **kwargs))]
    pub fn new(args: &Bound<'_, PyTuple>, kwargs: Option<&Bound<'_, PyDict>>) -> Self {
        PyTimestampGenerator {}
    }

    fn next_timestamp(&self) -> PyResult<i64> {
        Err(PyNotImplementedError::new_err("Method is not implemented"))
    }
}

struct InternalTimestampGenerator {
    py_timestamp_generator: Py<PyTimestampGenerator>,
}
impl TimestampGenerator for InternalTimestampGenerator {
    fn next_timestamp(&self) -> i64 {
        Python::attach(|py| {
            let py_generator = self.py_timestamp_generator.bind(py);

            py_generator
                .call_method0("next_timestamp")
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
        let generator = obj
            .extract::<Bound<PyTimestampGenerator>>()
            .map_err(|_| DriverSessionConfigError::invalid_timestamp_generator(obj))?;

        if let Ok(monotonic) = generator.cast::<PyMonotonicTimestampGenerator>() {
            return Ok(Self {
                inner: monotonic.borrow().inner.clone(),
            });
        }

        if let Ok(simple) = generator.cast::<PySimpleTimestampGenerator>() {
            return Ok(Self {
                inner: simple.borrow().inner.clone(),
            });
        }

        Ok(Self {
            inner: Arc::new(InternalTimestampGenerator {
                py_timestamp_generator: generator.to_owned().unbind(),
            }),
        })
    }
}

#[pyclass(extends=PyTimestampGenerator, name = "MonotonicTimestampGenerator", frozen)]
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
    ) -> PyClassInitializer<Self> {
        let mut monotonic_timestamp_generator = MonotonicTimestampGenerator::new()
            .with_warning_times(warning_threshold.0, warning_interval.0);

        if !warn_on_drift {
            monotonic_timestamp_generator = monotonic_timestamp_generator.without_warnings();
        }

        PyClassInitializer::from(PyTimestampGenerator {}).add_subclass(
            PyMonotonicTimestampGenerator {
                inner: Arc::new(monotonic_timestamp_generator),
            },
        )
    }
}

#[pyclass(extends=PyTimestampGenerator, name = "SimpleTimestampGenerator", frozen)]
struct PySimpleTimestampGenerator {
    inner: Arc<SimpleTimestampGenerator>,
}

#[pymethods]
impl PySimpleTimestampGenerator {
    #[new]
    pub fn new() -> PyClassInitializer<Self> {
        PyClassInitializer::from(PyTimestampGenerator {}).add_subclass(PySimpleTimestampGenerator {
            inner: Arc::new(SimpleTimestampGenerator {}),
        })
    }
}

#[pyclass(subclass, skip_from_py_object, name = "HostFilter", frozen)]
pub(crate) struct PyHostFilter {}

#[pymethods]
impl PyHostFilter {
    #[expect(unused_variables)]
    #[new]
    #[pyo3(signature = (*args, **kwargs))]
    pub fn new(args: &Bound<'_, PyTuple>, kwargs: Option<&Bound<'_, PyDict>>) -> Self {
        PyHostFilter {}
    }

    fn accept(&self, _peer: Py<PyPeer>) -> PyResult<bool> {
        Err(PyNotImplementedError::new_err("Method is not implemented"))
    }
}

struct InternalHostFilter {
    py_host_filter: Py<PyHostFilter>,
}
impl HostFilter for InternalHostFilter {
    fn accept(&self, peer: &Peer) -> bool {
        Python::attach(|py| {
            let py_filter = self.py_host_filter.bind(py);
            let py_peer = PyPeer::from(peer);

            py_filter
                .call_method1("accept", (py_peer,))
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
        if let Ok(host_filter) = obj.extract::<Bound<'_, PyHostFilter>>() {
            if let Ok(filter) = host_filter.cast::<PyAcceptAllHostFilter>() {
                return Ok(Self {
                    inner: filter.borrow().inner.clone(),
                });
            }

            if let Ok(filter) = host_filter.cast::<PyDcHostFilter>() {
                return Ok(Self {
                    inner: filter.borrow().inner.clone(),
                });
            }

            return Ok(Self {
                inner: Arc::new(InternalHostFilter {
                    py_host_filter: host_filter.to_owned().unbind(),
                }),
            });
        }

        if let Ok(points) = obj.extract::<NodeAddrs>() {
            let filter = AllowListHostFilter::new(points)
                .map_err(|_| DriverSessionConfigError::InvalidHostFilterAddress)?;
            return Ok(Self {
                inner: Arc::new(filter),
            });
        }

        Err(DriverSessionConfigError::invalid_host_filter(obj))
    }
}

#[pyclass(extends=PyHostFilter, name = "AcceptAllHostFilter", frozen)]
struct PyAcceptAllHostFilter {
    inner: Arc<AcceptAllHostFilter>,
}

#[pymethods]
impl PyAcceptAllHostFilter {
    #[new]
    pub fn new() -> PyClassInitializer<Self> {
        PyClassInitializer::from(PyHostFilter {}).add_subclass(PyAcceptAllHostFilter {
            inner: Arc::new(AcceptAllHostFilter {}),
        })
    }
}

#[pyclass(extends=PyHostFilter, name = "DcHostFilter", frozen)]
struct PyDcHostFilter {
    inner: Arc<DcHostFilter>,
}

#[pymethods]
impl PyDcHostFilter {
    #[new]
    pub fn new(local_dc: String) -> PyClassInitializer<Self> {
        PyClassInitializer::from(PyHostFilter {}).add_subclass(PyDcHostFilter {
            inner: Arc::new(DcHostFilter::new(local_dc)),
        })
    }
}

#[pyclass(get_all, frozen, name = "Peer")]
pub struct PyPeer {
    pub host_id: uuid::Uuid,
    pub address: (IpAddr, u16),
    //TODO when LBC is merged switch to using Token instead of i64
    pub tokens: Vec<i64>,
    pub datacenter: Option<String>,
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

impl From<&Peer> for PyPeer {
    fn from(peer: &Peer) -> Self {
        Self {
            host_id: peer.host_id,
            address: (peer.address.ip(), peer.address.port()),
            tokens: peer.tokens.iter().map(|t| t.value()).collect(),
            datacenter: peer.datacenter.clone(),
            rack: peer.rack.clone(),
        }
    }
}

#[pymodule]
pub(crate) fn policies(_py: Python<'_>, module: &Bound<'_, PyModule>) -> PyResult<()> {
    module.add_class::<PyAuthenticatorProvider>()?;
    module.add_class::<PyAuthenticator>()?;
    module.add_class::<PyUntranslatedPeer>()?;
    module.add_class::<PyAddressTranslator>()?;
    module.add_class::<PyTimestampGenerator>()?;
    module.add_class::<PyMonotonicTimestampGenerator>()?;
    module.add_class::<PySimpleTimestampGenerator>()?;
    module.add_class::<PyHostFilter>()?;
    module.add_class::<PyAcceptAllHostFilter>()?;
    module.add_class::<PyDcHostFilter>()?;
    module.add_class::<PyPeer>()?;
    Ok(())
}
