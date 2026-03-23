use pyo3::{prelude::*, types::PyString};
use scylla::routing::Token;

#[pyclass(name = "Token", frozen, from_py_object)]
#[derive(Clone)]
pub(crate) struct PyToken {
    pub(crate) _inner: Token,
}

#[pymethods]
impl PyToken {
    #[new]
    fn new(value: i64) -> Self {
        Self {
            _inner: Token::new(value),
        }
    }

    #[getter]
    fn value(&self) -> i64 {
        self._inner.value()
    }

    fn __repr__<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyString>> {
        PyString::from_fmt(py, format_args!("Token({})", self._inner.value()))
    }

    fn __eq__(&self, other: &PyToken) -> bool {
        self._inner == other._inner
    }

    fn __hash__(&self) -> i64 {
        self._inner.value()
    }
}

#[pymodule]
pub(crate) fn routing(_py: Python<'_>, module: &Bound<'_, PyModule>) -> PyResult<()> {
    module.add_class::<PyToken>()?;
    Ok(())
}
