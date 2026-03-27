use async_trait::async_trait;
use pyo3::exceptions::PyRuntimeError;
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
//TODO:
// Ask if We want to implement some of the Custom Authenticator that are implemented
// in python driver. This would improve performance for python users that would
// want to use them, as it would eliminate the need to call python code from rust for each authentication step.

#[derive(Clone)]
#[pyclass(subclass, skip_from_py_object, name = "Authenticator")]
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
        Err(PyRuntimeError::new_err("Method unimplemented"))
    }

    fn success(&self, _token: Option<&[u8]>) -> PyResult<()> {
        Ok(())
    }
}

#[derive(Clone)]
pub(crate) struct InternalAuthenticator {
    pub(crate) python_authenticator: Py<PyAuthenticator>,
}

#[async_trait]
impl AuthenticatorProvider for InternalAuthenticator {
    async fn start_authentication_session(
        &self,
        _authenticator_name: &str,
    ) -> Result<(Option<Vec<u8>>, Box<dyn AuthenticatorSession>), AuthError> {
        let (result, self_clone) = Python::attach(
            |py| -> PyResult<(Option<Vec<u8>>, Box<InternalAuthenticator>)> {
                let py_auth = self.python_authenticator.bind(py);

                let response = py_auth
                    .call_method0("initial_response")?
                    .extract::<Option<Vec<u8>>>()?;

                Ok((response, Box::new(self.clone())))
            },
        )
        .map_err(|e| format!("Python initial_response failed: {:?}", e))?;

        Ok((result, self_clone))
    }
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

    fn translate(&self, _addr: Py<PyPeerInfo>) -> PyResult<(IpAddr, u16)> {
        Err(PyRuntimeError::new_err("Method unimplemented"))
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
            let py_peer_info = PyPeerInfo::from(untranslated_peer);

            py_trans
                .call_method1("translate", (py_peer_info,))?
                .extract::<(IpAddr, u16)>()
        })
        .map_err(CustomTranslationError::new)?;

        Ok(SocketAddr::from(result))
    }
}

#[pyclass(get_all, name = "PeerInfo")]
pub struct PyPeerInfo {
    pub host_id: uuid::Uuid,
    pub address: (IpAddr, u16),
    pub datacenter: Option<String>,
    pub rack: Option<String>,
}

#[pymethods]
impl PyPeerInfo {
    fn __repr__(&self, py: Python<'_>) -> PyResult<Py<PyString>> {
        let (ip, port) = self.address;

        let repr_str = PyString::from_fmt(
            py,
            format_args!(
                "PeerInfo(host_id='{}', address=('{}', {}), datacenter={:?}, rack={:?})",
                self.host_id, ip, port, self.datacenter, self.rack
            ),
        )?;

        Ok(repr_str.into())
    }
}

impl From<&UntranslatedPeer<'_>> for PyPeerInfo {
    fn from(peer: &UntranslatedPeer) -> Self {
        Self {
            host_id: peer.host_id(),
            address: (
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
        Err(PyRuntimeError::new_err("Method unimplemented"))
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
        Err(PyRuntimeError::new_err("Method unimplemented"))
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
    module.add_class::<PyAuthenticator>()?;
    module.add_class::<PyPeerInfo>()?;
    module.add_class::<PyAddressTranslator>()?;
    module.add_class::<PyTimestampGenerator>()?;
    module.add_class::<PyHostFilter>()?;
    module.add_class::<PyPeer>()?;
    Ok(())
}
