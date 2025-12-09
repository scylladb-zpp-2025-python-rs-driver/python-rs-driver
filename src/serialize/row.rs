use crate::serialize::value::{
    PyAnyWrapper, PythonDriverSerializationError, mk_typck_err_named_row, mk_typck_err_row,
};
use pyo3::exceptions::PyTypeError;
use pyo3::prelude::*;
use pyo3::types::{PyDict, PyList, PyTuple};
use pyo3::{Bound, Py, PyAny, Python};
use scylla_cql::_macro_internal::{
    ColumnSpec, RowSerializationContext, RowWriter, SerializationError, SerializeRow,
    SerializeValue,
};
use std::ops::Deref;

pub(crate) struct PyAnyWrapperRow(pub(crate) Py<PyAny>);

impl Deref for PyAnyWrapperRow {
    type Target = Py<PyAny>;
    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl PyAnyWrapperRow {
    fn length_equality_check(
        row_len: usize,
        cols_len: usize,
        name: &'static str,
    ) -> Result<(), SerializationError> {
        if row_len != cols_len {
            Err(SerializationError::new(mk_typck_err_named_row(
                name,
                scylla::serialize::row::BuiltinTypeCheckErrorKind::WrongColumnCount {
                    rust_cols: row_len,
                    cql_cols: cols_len,
                },
            )))
        } else {
            Ok(())
        }
    }

    fn serialize_element(
        col: &ColumnSpec,
        val: Bound<PyAny>,
        row_writer: &mut RowWriter,
    ) -> Result<(), SerializationError> {
        let wrapper = PyAnyWrapper(val);
        let sub_writer = row_writer.make_cell_writer();
        SerializeValue::serialize(&wrapper, col.typ(), sub_writer)?;

        Ok(())
    }
}

impl SerializeRow for PyAnyWrapperRow {
    fn serialize(
        &self,
        ctx: &RowSerializationContext<'_>,
        row_writer: &mut RowWriter,
    ) -> Result<(), SerializationError> {
        Python::with_gil(|py| {
            let val = self.bind(py);

            if let Ok(row) = val.downcast::<PyTuple>() {
                Self::length_equality_check(
                    row.len(),
                    ctx.columns().len(),
                    std::any::type_name::<PyTuple>(),
                )?;
                for (col, val) in ctx.columns().iter().zip(row.iter()) {
                    Self::serialize_element(col, val, row_writer)?;
                }
                Ok(())
            } else if let Ok(row) = val.downcast::<PyList>() {
                Self::length_equality_check(
                    row.len(),
                    ctx.columns().len(),
                    std::any::type_name::<PyList>(),
                )?;
                for (col, val) in ctx.columns().iter().zip(row.iter()) {
                    Self::serialize_element(col, val, row_writer)?;
                }
                Ok(())
            } else if let Ok(row) = val.downcast::<PyDict>() {
                Self::length_equality_check(
                    row.len(),
                    ctx.columns().len(),
                    std::any::type_name::<PyDict>(),
                )?;
                for col in ctx.columns().iter() {
                    let item: Bound<PyAny> = row
                        .get_item(col.name())
                        .map_err(|e| {
                            SerializationError::new(PythonDriverSerializationError::PythonError(e))
                        })?
                        .ok_or_else(|| {
                            SerializationError::new(mk_typck_err_row::<PyDict>(
                                scylla::serialize::row::BuiltinTypeCheckErrorKind::ValueMissingForColumn {
                                    name: col.name().into()
                                }))
                        })?;
                    Self::serialize_element(col, item, row_writer)?;
                }

                Ok(())
            } else {
                Err(SerializationError::new(PyTypeError::new_err(
                    "expected Python tuple, list or dict",
                )))
            }
        })
    }

    fn is_empty(&self) -> bool {
        Python::with_gil(|py| self.is_none(py))
    }
}
