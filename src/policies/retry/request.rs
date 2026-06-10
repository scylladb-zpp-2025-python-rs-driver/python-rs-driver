use crate::enums::PyConsistency;
use crate::policies::retry::errors::PyRequestAttemptError;
use pyo3::prelude::*;
use scylla::errors::RequestAttemptError;
use scylla::policies::retry::RequestInfo;

#[pyclass(name = "RequestInfo", frozen, from_py_object)]
#[derive(Debug, Clone)]
pub(crate) struct PyRequestInfo {
    #[pyo3(get)]
    pub(crate) error: PyRequestAttemptError,
    #[pyo3(get)]
    pub(crate) is_idempotent: bool,
    #[pyo3(get)]
    pub(crate) consistency: PyConsistency,
    rust_error: RequestAttemptError,
}

impl From<&RequestInfo<'_>> for PyRequestInfo {
    fn from(value: &RequestInfo<'_>) -> Self {
        Self {
            error: value.error.clone().into(),
            is_idempotent: value.is_idempotent,
            consistency: value.consistency.into(),
            rust_error: value.error.clone(),
        }
    }
}

impl PyRequestInfo {
    pub fn to_request_info<'a>(&'a self) -> RequestInfo<'a> {
        RequestInfo::new(
            &self.rust_error,
            self.is_idempotent,
            self.consistency.into(),
        )
    }
}

#[pymethods]
impl PyRequestInfo {
    #[new]
    pub fn new(
        error: PyRequestAttemptError,
        is_idempotent: bool,
        consistency: PyConsistency,
    ) -> Self {
        Self {
            error: error.clone(),
            is_idempotent,
            consistency,
            rust_error: error.into(),
        }
    }
}
