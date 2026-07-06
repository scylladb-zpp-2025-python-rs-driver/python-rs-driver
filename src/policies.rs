use async_trait::async_trait;
use pyo3::exceptions::PyNotImplementedError;
use pyo3::prelude::{PyAnyMethods, PyModule, PyModuleMethods};
use pyo3::sync::PyOnceLock;
use pyo3::types::{PyDict, PyString, PyTuple};
use pyo3::{
    Borrowed, Bound, BoundObject, FromPyObject, Py, PyAny, PyResult, Python, intern, pyclass,
    pymethods, pymodule,
};
use scylla::authentication::{AuthError, AuthenticatorProvider, AuthenticatorSession};
use scylla::cluster::metadata::Peer;
use scylla::errors::{CustomTranslationError, TranslationError};
use scylla::policies::address_translator::{AddressTranslator, UntranslatedPeer};
use scylla::policies::host_filter::HostFilter;
use scylla::policies::timestamp_generator::TimestampGenerator;
use std::net::{IpAddr, SocketAddr};
use std::time::{SystemTime, UNIX_EPOCH};

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


    }

    }
}

/// Stores a Python object with a `translate` method (user's custom implementation)
/// and implements the Rust `AddressTranslator` trait by delegating to that Python object.
struct CustomAddressTranslator {
    inner: Py<PyAny>,
}

#[async_trait]
impl AddressTranslator for CustomAddressTranslator {
    async fn translate_address(
        &self,
        untranslated_peer: &UntranslatedPeer,
    ) -> Result<SocketAddr, TranslationError> {
        Python::attach(|py| -> PyResult<SocketAddr> {
            let py_trans = self.inner.bind(py);
            let peer_info = PyUntranslatedPeer::from(untranslated_peer);

            let translated = py_trans
                .call_method1(intern!(py, "translate"), (peer_info,))?
                .extract::<ContactPoint>()?;

            SocketAddr::try_from(translated).map_err(|e| e.into())
        })
        .map_err(|e| CustomTranslationError::new(e).into())
    }
}

/// Python-facing input type for address translator. Extracts from a built-in `PyDictAddressTranslator`
/// or wraps any Python object with a `translate` method as a `CustomAddressTranslator`.
pub(crate) struct PyAddressTranslator {
    inner: Arc<dyn AddressTranslator>,
}

impl PyAddressTranslator {
    pub(crate) fn into_inner(self) -> Arc<dyn AddressTranslator> {
        self.inner
    }
}

impl<'py> FromPyObject<'_, 'py> for PyAddressTranslator {
    type Error = DriverSessionConfigError;

    fn extract(obj: Borrowed<'_, 'py, PyAny>) -> Result<Self, Self::Error> {
        if !obj.hasattr(intern!(obj.py(), "translate")).unwrap_or(false) {
            return Err(DriverSessionConfigError::invalid_address_translator(obj));
        }

        Ok(Self {
            inner: Arc::new(CustomAddressTranslator {
                inner: obj.unbind(),
            }),
        })
    }
}
/// Python representation of an untranslated peer address, exposing host_id, untranslated_address,
/// datacenter, and rack. Exposed to Python as `UntranslatedPeer`.
#[pyclass(name = "UntranslatedPeer", frozen)]
pub struct PyUntranslatedPeer {
    host_id: uuid::Uuid,
    untranslated_address: (IpAddr, u16),
    datacenter: Option<String>,
    rack: Option<String>,

    // Cached Python-side representations used by the getters.
    pub py_host_id: PyOnceLock<Py<PyAny>>,
    pub py_untranslated_address: PyOnceLock<Py<PyTuple>>,
    pub py_datacenter: PyOnceLock<Py<PyString>>,
    pub py_rack: PyOnceLock<Py<PyString>>,
}

#[pymethods]
impl PyUntranslatedPeer {
    #[getter]
    fn host_id(&self, py: Python<'_>) -> PyResult<Py<PyAny>> {
        Ok(self
            .py_host_id
            .get_or_try_init(py, || {
                Ok::<_, PyErr>(self.host_id.into_pyobject(py)?.unbind())
            })?
            .clone_ref(py))
    }

    #[getter]
    fn untranslated_address(&self, py: Python<'_>) -> PyResult<Py<PyTuple>> {
        Ok(self
            .py_untranslated_address
            .get_or_try_init(py, || {
                let (ip, port) = self.untranslated_address;

                Ok::<_, PyErr>(
                    (ip, port)
                        .into_pyobject(py)?
                        .cast_into::<PyTuple>()?
                        .unbind(),
                )
            })?
            .clone_ref(py))
    }

    #[getter]
    fn datacenter(&self, py: Python<'_>) -> Py<PyAny> {
        match &self.datacenter {
            None => py.None(),
            Some(datacenter) => self
                .py_datacenter
                .get_or_init(py, || PyString::new(py, datacenter).unbind())
                .clone_ref(py)
                .into_any(),
        }
    }

    #[getter]
    fn rack(&self, py: Python<'_>) -> Py<PyAny> {
        match &self.rack {
            None => py.None(),
            Some(rack) => self
                .py_rack
                .get_or_init(py, || PyString::new(py, rack).unbind())
                .clone_ref(py)
                .into_any(),
        }
    }

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

            py_host_id: PyOnceLock::new(),
            py_untranslated_address: PyOnceLock::new(),
            py_datacenter: PyOnceLock::new(),
            py_rack: PyOnceLock::new(),
        }
    }
}

impl<'a> From<&'a PyUntranslatedPeer> for UntranslatedPeer<'a> {
    fn from(peer: &'a PyUntranslatedPeer) -> UntranslatedPeer<'a> {
        let (ip, port) = peer.untranslated_address;

        UntranslatedPeer::from_fields(
            peer.host_id,
            SocketAddr::new(ip, port),
            peer.datacenter.as_deref(),
            peer.rack.as_deref(),
        )
    }
}

#[pyclass(subclass, skip_from_py_object, name = "TimestampGenerator")]
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

pub(crate) struct InternalTimestampGenerator {
    pub(crate) py_timestamp_generator: Py<PyTimestampGenerator>,
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

#[pyclass(subclass, skip_from_py_object, name = "HostFilter")]
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

pub(crate) struct InternalHostFilter {
    pub(crate) py_host_filter: Py<PyHostFilter>,
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
    module.add_class::<PyTimestampGenerator>()?;
    module.add_class::<PyHostFilter>()?;
    module.add_class::<PyPeer>()?;
    Ok(())
}
