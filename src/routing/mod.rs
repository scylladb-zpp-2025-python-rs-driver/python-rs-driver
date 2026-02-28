use pyo3::prelude::*;

pub(crate) mod sharding;
use sharding::Sharder;

#[pyclass(frozen)]
#[derive(Clone)]
pub(crate) struct Token {
    pub(crate) _inner: scylla::routing::Token,
}

#[pymethods]
impl Token {
    #[new]
    fn new(value: i64) -> Self {
        Self {
            _inner: scylla::routing::Token::new(value),
        }
    }

    fn value(&self) -> i64 {
        self._inner.value()
    }
}

#[pymodule]
pub(crate) fn routing(_py: Python<'_>, module: &Bound<'_, PyModule>) -> PyResult<()> {
    module.add_class::<Sharder>()?;
    module.add_class::<Token>()?;
    Ok(())
}
