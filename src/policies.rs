use async_trait::async_trait;
use pyo3::exceptions::PyNotImplementedError;
use pyo3::prelude::{PyAnyMethods, PyModule, PyModuleMethods};
use pyo3::types::{PyDict, PyTuple};
use pyo3::{Bound, Py, PyResult, Python, pyclass, pymethods, pymodule};
use scylla::authentication::{AuthError, AuthenticatorProvider, AuthenticatorSession};

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

#[pymodule]
pub(crate) fn policies(_py: Python<'_>, module: &Bound<'_, PyModule>) -> PyResult<()> {
    module.add_class::<PyAuthenticatorProvider>()?;
    module.add_class::<PyAuthenticator>()?;
    Ok(())
}
