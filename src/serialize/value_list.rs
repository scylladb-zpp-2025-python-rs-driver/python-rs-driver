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

// TODO: Refactor this wrapper into an enum with variants for `PyList`, `PyTuple`, and `PyDict`.
// Currently, the type is checked in `session::try_into_value_list`, but this information
// is lost when stored as `PyAny`.
pub(crate) struct PyAnyWrapperValueList {
    pub(crate) inner: Py<PyAny>,
    pub(crate) is_empty: bool,
}

impl Deref for PyAnyWrapperValueList {
    type Target = Py<PyAny>;
    fn deref(&self) -> &Self::Target {
        &self.inner
    }
}

impl PyAnyWrapperValueList {
    fn length_equality_check<T: Any>(
        val_list_len: usize,
        cols_len: usize,
    ) -> Result<(), SerializationError> {
        if val_list_len != cols_len {
            return Err(SerializationError::new(mk_typck_err_val_list::<T>(
                BuiltinTypeCheckErrorKind::WrongColumnCount {
                    rust_cols: val_list_len,
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
        value_list: &Bound<'py, PyAny>,
        ctx: &RowSerializationContext<'_>,
        row_writer: &mut RowWriter,
    ) -> Result<(), SerializationError> {
        let len = value_list
            .len()
            .map_err(|e| SerializationError::new(PythonDriverSerializationError::PythonError(e)))?;

        Self::length_equality_check::<T>(len, ctx.columns().len())?;

        let iter = value_list
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
        value_list: &Bound<'py, PyDict>,
        ctx: &RowSerializationContext<'_>,
        row_writer: &mut RowWriter,
    ) -> Result<(), SerializationError> {
        Self::length_equality_check::<PyDict>(value_list.len(), ctx.columns().len())?;

        for col in ctx.columns().iter() {
            let item: Bound<PyAny> = value_list
                .get_item(col.name())
                .map_err(|e| {
                    SerializationError::new(PythonDriverSerializationError::PythonError(e))
                })?
                .ok_or_else(|| {
                    SerializationError::new(mk_typck_err_val_list::<PyDict>(
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

impl SerializeRow for PyAnyWrapperValueList {
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
            } else if let Ok(value_list) = val.cast::<PyDict>() {
                Self::serialize_dict(value_list, ctx, row_writer)
            } else {
                Err(SerializationError::new(PyTypeError::new_err(
                    "expected Python tuple, list or dict",
                )))
            }
        })
    }

    fn is_empty(&self) -> bool {
        self.is_empty
    }
}

pub(crate) fn mk_typck_err_val_list<T>(
    kind: impl Into<BuiltinTypeCheckErrorKind>,
) -> SerializationError {
    SerializationError::new(BuiltinTypeCheckError {
        rust_name: std::any::type_name::<T>(),
        kind: kind.into(),
    })
}
