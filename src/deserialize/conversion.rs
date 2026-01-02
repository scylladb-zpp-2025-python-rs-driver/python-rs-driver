use pyo3::sync::PyOnceLock;
use pyo3::types::{PyAnyMethods, PyDict, PyInt, PyType};
use pyo3::{Bound, IntoPyObject, Py, PyAny, PyErr, PyResult, Python, ffi};
use scylla_cql::value::{CqlDuration, CqlVarintBorrowed};

fn get_relative_delta_cls(py: Python<'_>) -> PyResult<&Bound<'_, PyType>> {
    static RELATIVEDELTA_CLS: PyOnceLock<Py<PyType>> = PyOnceLock::new();
    RELATIVEDELTA_CLS.import(py, "dateutil.relativedelta", "relativedelta")
}

pub(crate) struct CqlVarintWrapper<'b> {
    val: CqlVarintBorrowed<'b>,
}

impl<'b> From<CqlVarintBorrowed<'b>> for CqlVarintWrapper<'b> {
    fn from(val: CqlVarintBorrowed<'b>) -> Self {
        Self { val }
    }
}

impl<'py> IntoPyObject<'py> for CqlVarintWrapper<'_> {
    type Target = PyInt;
    type Output = Bound<'py, Self::Target>;
    type Error = PyErr;

    fn into_pyobject(self, py: Python<'py>) -> Result<Self::Output, Self::Error> {
        let bytes = self.val.as_signed_bytes_be_slice();
        unsafe {
            let val = ffi::_PyLong_FromByteArray(bytes.as_ptr(), bytes.len(), 0, 1);

            Ok(Bound::from_owned_ptr(py, val).cast_into()?)
        }
    }
}

pub(crate) struct CqlDurationWrapper {
    val: CqlDuration,
}

impl From<CqlDuration> for CqlDurationWrapper {
    fn from(val: CqlDuration) -> Self {
        Self { val }
    }
}

impl<'py> IntoPyObject<'py> for CqlDurationWrapper {
    type Target = PyAny;
    type Output = Bound<'py, Self::Target>;
    type Error = PyErr;

    fn into_pyobject(self, py: Python<'py>) -> Result<Self::Output, Self::Error> {
        let cls = get_relative_delta_cls(py)?;
        let duration = &self.val;
        let kwargs = PyDict::new(py);
        kwargs.set_item("months", duration.months)?;
        kwargs.set_item("days", duration.days)?;
        kwargs.set_item("microseconds", duration.nanoseconds / 1000)?;

        cls.call((), Some(&kwargs))
    }
}
