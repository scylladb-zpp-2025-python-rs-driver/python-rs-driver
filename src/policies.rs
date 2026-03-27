use async_trait::async_trait;
use pyo3::exceptions::PyNotImplementedError;
use pyo3::prelude::{PyAnyMethods, PyModule, PyModuleMethods};
use pyo3::types::{PyDict, PyString, PyTuple};
use pyo3::{Bound, Py, PyResult, Python, pyclass, pymethods, pymodule};
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

#[pyclass(subclass, skip_from_py_object, name = "AddressTranslator")]
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

pub(crate) struct InternalAddressTranslator {
    pub(crate) python_translator: Py<PyAddressTranslator>,
}

#[async_trait]
impl AddressTranslator for InternalAddressTranslator {
    async fn translate_address(
        &self,
        untranslated_peer: &UntranslatedPeer,
    ) -> Result<SocketAddr, TranslationError> {
        let result = Python::attach(|py| -> PyResult<(IpAddr, u16)> {
            let py_trans = self.python_translator.bind(py);
            let py_peer_info = PyUntranslatedPeer::from(untranslated_peer);

            py_trans
                .call_method1("translate", (py_peer_info,))?
                .extract::<(IpAddr, u16)>()
        })
        .map_err(CustomTranslationError::new)?;

        Ok(SocketAddr::from(result))
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
    module.add_class::<PyAddressTranslator>()?;
    module.add_class::<PyTimestampGenerator>()?;
    module.add_class::<PyHostFilter>()?;
    module.add_class::<PyPeer>()?;
    Ok(())
}
