use std::any::Any;
use std::ops::Deref;

use pyo3::exceptions::PyTypeError;
use pyo3::prelude::*;
use pyo3::types::{PyDict, PyList, PyTuple};
use pyo3::{Bound, Py, PyAny};

use scylla::errors::SerializationError;
use scylla::frame::response::result::ColumnSpec;
use scylla::serialize::row::{
    BuiltinTypeCheckError, BuiltinTypeCheckErrorKind, RowSerializationContext, SerializeRow,
};
use scylla::serialize::value::SerializeValue;
use scylla::serialize::writers::{RowWriter, WrittenCellProof};

use crate::serialize::value::{PyAnyWrapper, PythonDriverSerializationError};

pub(crate) struct PyAnyWrapperRow(pub(crate) Py<PyAny>);

impl Deref for PyAnyWrapperRow {
    type Target = Py<PyAny>;
    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl PyAnyWrapperRow {
    fn length_equality_check<T: Any>(
        row_len: usize,
        cols_len: usize,
    ) -> Result<(), SerializationError> {
        if row_len != cols_len {
            return Err(SerializationError::new(mk_typck_err_row::<T>(
                BuiltinTypeCheckErrorKind::WrongColumnCount {
                    rust_cols: row_len,
                    cql_cols: cols_len,
                },
            )));
        }

        Ok(())
    }

    fn serialize_element<'a>(
        col: &ColumnSpec,
        val: &Bound<PyAny>,
        row_writer: &'a mut RowWriter<'_>,
    ) -> Result<WrittenCellProof<'a>, SerializationError> {
        let wrapper = PyAnyWrapper::new(val);
        let sub_writer = row_writer.make_cell_writer();
        SerializeValue::serialize(&wrapper, col.typ(), sub_writer)
    }

    fn serialize_sequence<'py, T: Any>(
        row: &Bound<'py, PyAny>,
        ctx: &RowSerializationContext<'_>,
        row_writer: &mut RowWriter,
    ) -> Result<(), SerializationError> {
        let len = row
            .len()
            .map_err(|e| SerializationError::new(PythonDriverSerializationError::PythonError(e)))?;

        Self::length_equality_check::<T>(len, ctx.columns().len())?;

        let iter = row
            .try_iter()
            .map_err(|e| SerializationError::new(PythonDriverSerializationError::PythonError(e)))?;

        for (col, val) in ctx.columns().iter().zip(iter) {
            let val = val.map_err(|e| {
                SerializationError::new(PythonDriverSerializationError::PythonError(e))
            })?;
            Self::serialize_element(col, &val, row_writer)?;
        }

        Ok(())
    }

    fn serialize_dict<'py>(
        row: &Bound<'py, PyDict>,
        ctx: &RowSerializationContext<'_>,
        row_writer: &mut RowWriter,
    ) -> Result<(), SerializationError> {
        Self::length_equality_check::<PyDict>(row.len(), ctx.columns().len())?;

        for col in ctx.columns().iter() {
            let item: Bound<PyAny> = row
                .get_item(col.name())
                .map_err(|e| {
                    SerializationError::new(PythonDriverSerializationError::PythonError(e))
                })?
                .ok_or_else(|| {
                    SerializationError::new(mk_typck_err_row::<PyDict>(
                        BuiltinTypeCheckErrorKind::ValueMissingForColumn {
                            name: col.name().into(),
                        },
                    ))
                })?;
            Self::serialize_element(col, &item, row_writer)?;
        }

        Ok(())
    }
}

impl SerializeRow for PyAnyWrapperRow {
    fn serialize(
        &self,
        ctx: &RowSerializationContext<'_>,
        row_writer: &mut RowWriter,
    ) -> Result<(), SerializationError> {
        Python::attach(|py| {
            let val = self.bind(py);

            if val.is_instance_of::<PyList>() {
                Self::serialize_sequence::<PyList>(val, ctx, row_writer)
            } else if val.is_instance_of::<PyTuple>() {
                Self::serialize_sequence::<PyTuple>(val, ctx, row_writer)
            } else if let Ok(row) = val.cast::<PyDict>() {
                Self::serialize_dict(row, ctx, row_writer)
            } else {
                Err(SerializationError::new(PyTypeError::new_err(
                    "expected Python tuple, list or dict",
                )))
            }
        })
    }

    fn is_empty(&self) -> bool {
        Python::attach(|py| {
            if self.is_none(py) {
                return true;
            }

            self.bind(py).len().map(|len| len == 0).unwrap_or(false)
        })
    }
}

pub(crate) fn mk_typck_err_row<T>(
    kind: impl Into<BuiltinTypeCheckErrorKind>,
) -> SerializationError {
    SerializationError::new(BuiltinTypeCheckError {
        rust_name: std::any::type_name::<T>(),
        kind: kind.into(),
    })
}
