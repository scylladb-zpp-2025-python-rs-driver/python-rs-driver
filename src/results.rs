use crate::value::{PyDeserializeValue, PyDeserializedValue};
use bytes::Bytes;
use pyo3::exceptions::{PyRuntimeError, PyTypeError};
use pyo3::prelude::{PyDictMethods, PyModule, PyModuleMethods};
use pyo3::types::{PyDict, PyString};
use pyo3::{
    Bound, IntoPyObject, Py, PyAny, PyErr, PyRefMut, PyResult, Python, pyclass, pymethods, pymodule,
};
use scylla::response::query_result::QueryRowsResult;
use scylla_cql::_macro_internal::{ColumnIterator, FrameSlice};
use scylla_cql::deserialize::result::RawRowIterator;
use scylla_cql::frame::response::result::{DeserializedMetadataAndRawRows, ResultMetadataHolder};
use scylla_cql::value::Row;
use stable_deref_trait::StableDeref;
use std::fmt::Write;
use std::ops::Deref;
use yoke::{Yoke, Yokeable};

// Originally written by @Lorak-mmk, moved by @patrycja-ziemkiewicz
#[pyclass]
pub(crate) struct RequestResult {
    pub(crate) inner: scylla::response::query_result::QueryResult,
}

#[pymethods]
impl RequestResult {
    fn __str__<'gil>(&mut self, py: Python<'gil>) -> PyResult<Bound<'gil, PyString>> {
        let mut result = String::new();
        let rows_result = match self.inner.clone().into_rows_result() {
            Ok(r) => r,
            Err(e) => return Ok(PyString::new(py, &format!("non-rows result: {}", e))),
        };
        for r in rows_result.rows::<Row>().map_err(|e| {
            PyRuntimeError::new_err(format!("Failed to deserialize metadata: {}", e))
        })? {
            let row = match r {
                Ok(r) => r,
                Err(e) => {
                    return Err(PyRuntimeError::new_err(format!(
                        "Failed to deserialize row: {}",
                        e
                    )));
                }
            };
            write!(result, "|").unwrap();
            for col in row.columns {
                match col {
                    Some(c) => write!(result, "{}", c).unwrap(),
                    None => write!(result, "null").unwrap(),
                };
                write!(result, "|").unwrap();
            }
            writeln!(result).unwrap();
        }
        Ok(PyString::new(py, &result))
    }

    #[pyo3(signature = (factory=None))]
    fn create_rows_result<'py>(
        &self,
        py: Python<'py>,
        factory: Option<Bound<RowFactory>>,
    ) -> PyResult<RowsResult> {
        let row_col_iterator = Py::new(
            py,
            RowColumnIterator::new(
                self.inner
                    .clone()
                    .into_rows_result()
                    .map_err(|e| PyTypeError::new_err(e.to_string()))?,
            ),
        )?;

        let f: Py<RowFactory> = match factory {
            Some(bound) => bound.unbind(),
            None => Py::new(py, RowFactory::new())?,
        };

        Ok(RowsResult {
            row_col_iterator,
            factory: f,
        })
    }
}
#[pyclass]
pub struct RowsResult {
    row_col_iterator: Py<RowColumnIterator>,
    factory: Py<RowFactory>,
}

#[pymethods]
impl RowsResult {
    pub fn __next__(&mut self) -> PyResult<Py<PyAny>> {
        Python::with_gil(|py| {
            self.row_col_iterator
                .borrow_mut(py)
                .yoked
                .with_mut_return(|view| view.next_row())?;

            self.factory
                .call_method1(py, "build", (&self.row_col_iterator.bind(py),))
        })
    }

    pub fn __iter__(slf: PyRefMut<'_, Self>) -> PyRefMut<'_, Self> {
        slf
    }
}

#[derive(Yokeable)]
struct IteratorWrapper<'a> {
    row_iterator: RawRowIterator<'a, 'a>,
    column_iterator: ColumnIterator<'a, 'a>,
}
impl<'a> IteratorWrapper<'a> {
    fn next_column(&mut self) -> PyResult<Column> {
        Python::with_gil(|py| {
            let raw_col = self
                .column_iterator
                .next()
                .ok_or_else(|| PyErr::new::<pyo3::exceptions::PyStopIteration, _>(""))?
                .map_err(|e| PyRuntimeError::new_err(e.to_string()))?;

            let val = PyDeserializedValue::deserialize_py(raw_col.spec.typ(), raw_col.slice, py)
                .map_err(|e| PyRuntimeError::new_err(e.to_string()))?;

            let column_name = PyString::new(py, raw_col.spec.name()).unbind();
            let value = val.into_pyobject(py).expect("Can't fail").unbind();

            Ok(Column { column_name, value })
        })
    }

    fn next_row(&mut self) -> PyResult<()> {
        let column_iterator = self
            .row_iterator
            .next()
            .ok_or_else(|| PyErr::new::<pyo3::exceptions::PyStopIteration, _>(""))?
            .map_err(|e| PyRuntimeError::new_err(e.to_string()))?;

        self.column_iterator = column_iterator;
        Ok(())
    }
}

// NOTE: In method attach_to_cart we cannot move out cart so We cannot use into_inner inside of it.
// Fields in DeserializedMetadataAndRawRows are private We have to think
// If it would be better to use method into_inner and construct new RawMetadata struct
// or to implement some public method in rust driver that would give access to raw_rows.
struct RawMetadata {
    metadata: ResultMetadataHolder,
    rows_count: usize,
    raw_rows: Bytes,
}

impl RawMetadata {
    fn from_inner(data: DeserializedMetadataAndRawRows) -> RawMetadata {
        let (metadata, rows_count, raw_rows) = data.into_inner();

        RawMetadata {
            metadata,
            rows_count,
            raw_rows,
        }
    }
}

struct RawMetadataCart(Box<RawMetadata>);

impl Deref for RawMetadataCart {
    type Target = RawMetadata;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}
unsafe impl StableDeref for RawMetadataCart {}

#[pyclass(name = "ColumnIterator")]
pub struct RowColumnIterator {
    yoked: Yoke<IteratorWrapper<'static>, RawMetadataCart>,
}

impl RowColumnIterator {
    fn new(result: QueryRowsResult) -> Self {
        let (data, _, _, _) = result.into_inner();

        let cart = RawMetadataCart(Box::new(RawMetadata::from_inner(data)));

        let yoked = Yoke::attach_to_cart(cart, |cart| {
            let frame_slice = FrameSlice::new(&cart.raw_rows);
            let row_iterator = RawRowIterator::new(
                cart.rows_count,
                cart.metadata.inner().col_specs(),
                frame_slice,
            );

            let column_iterator =
                ColumnIterator::new(cart.metadata.inner().col_specs(), frame_slice);

            IteratorWrapper {
                row_iterator,
                column_iterator,
            }
        });

        Self { yoked }
    }
}

#[pymethods]
impl RowColumnIterator {
    pub fn __next__(&mut self) -> PyResult<Column> {
        self.yoked.with_mut_return(|view| view.next_column())
    }
    pub fn __iter__(slf: PyRefMut<'_, Self>) -> PyRefMut<'_, Self> {
        slf
    }
}

#[pyclass]
pub struct Column {
    #[pyo3(get)]
    column_name: Py<PyString>,
    #[pyo3(get)]
    value: Py<PyAny>,
}
#[pyclass(subclass)]
pub struct RowFactory {}
#[pymethods]
impl RowFactory {
    #[new]
    pub fn new() -> Self {
        RowFactory {}
    }
    pub fn build<'py>(
        &self,
        py: Python<'py>,
        column_iterator: &Bound<'py, RowColumnIterator>,
    ) -> PyResult<Py<PyDict>> {
        let mut columns = column_iterator.borrow_mut();

        let dict = PyDict::new(py);
        loop {
            match columns.__next__() {
                Ok(column) => {
                    dict.set_item(column.column_name, column.value)?;
                }
                Err(err) => {
                    if err.is_instance_of::<pyo3::exceptions::PyStopIteration>(py) {
                        break;
                    } else {
                        return Err(err);
                    }
                }
            }
        }

        Ok(dict.into())
    }
}

impl Default for RowFactory {
    fn default() -> Self {
        Self::new()
    }
}

#[pymodule]
pub(crate) fn results(_py: Python<'_>, module: &Bound<'_, PyModule>) -> PyResult<()> {
    module.add_class::<RowFactory>()?;
    module.add_class::<Column>()?;
    module.add_class::<RequestResult>()?;
    module.add_class::<RowColumnIterator>()?;
    module.add_class::<RowsResult>()?;

    Ok(())
}
