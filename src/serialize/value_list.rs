use std::any::Any;

use pyo3::exceptions::{PyKeyError, PyTypeError};
use pyo3::prelude::*;
use pyo3::types::{PyDict, PyList, PyMapping, PySequence, PyTuple};
use pyo3::{Bound, BoundObject, Py, PyAny};

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
pub(crate) struct PyValueList {
    pub(crate) inner: Py<PyAny>,
    pub(crate) is_empty: bool,
}

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

fn serialize_sequence<'py>(
    value_list: &Bound<'py, PySequence>,
    ctx: &RowSerializationContext<'_>,
    row_writer: &mut RowWriter,
) -> Result<(), SerializationError> {
    let len = value_list
        .len()
        .map_err(|e| SerializationError::new(PythonDriverSerializationError::PythonError(e)))?;

    length_equality_check::<PySequence>(len, ctx.columns().len())?;

    let iter = value_list
        .try_iter()
        .map_err(|e| SerializationError::new(PythonDriverSerializationError::PythonError(e)))?;

    for (col, val) in ctx.columns().iter().zip(iter) {
        let val = val
            .map_err(|e| SerializationError::new(PythonDriverSerializationError::PythonError(e)))?;
        serialize_element(col, &val, row_writer)?;
    }

    Ok(())
}

fn serialize_mapping<'py>(
    value_list: &Bound<'py, PyMapping>,
    ctx: &RowSerializationContext<'_>,
    row_writer: &mut RowWriter,
) -> Result<(), SerializationError> {
    let py = value_list.py();
    let dict_len = value_list
        .len()
        .map_err(|e| SerializationError::new(PythonDriverSerializationError::PythonError(e)))?;
    length_equality_check::<PyDict>(dict_len, ctx.columns().len())?;

    for col in ctx.columns().iter() {
        let item: Bound<PyAny> = value_list.get_item(col.name()).map_err(|e| {
            if e.is_instance_of::<PyKeyError>(py) {
                SerializationError::new(mk_typck_err_val_list::<PyDict>(
                    BuiltinTypeCheckErrorKind::ValueMissingForColumn {
                        name: col.name().into(),
                    },
                ))
            } else {
                SerializationError::new(PythonDriverSerializationError::PythonError(e))
            }
        })?;
        serialize_element(col, &item, row_writer)?;
    }

    Ok(())
}

impl SerializeRow for PyValueList {
    fn serialize(
        &self,
        ctx: &RowSerializationContext<'_>,
        row_writer: &mut RowWriter,
    ) -> Result<(), SerializationError> {
        Python::attach(|py| {
            let val = self.inner.bind(py);

            if let Ok(sequence) = val.cast::<PySequence>() {
                serialize_sequence(sequence, ctx, row_writer)
            } else if let Ok(mapping) = val.cast::<PyMapping>() {
                serialize_mapping(mapping, ctx, row_writer)
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

impl<'a, 'py> FromPyObject<'a, 'py> for PyValueList {
    type Error = PyErr;

    fn extract(val: Borrowed<'a, 'py, PyAny>) -> Result<Self, Self::Error> {
        if val.is_instance_of::<PyList>()
            || val.is_instance_of::<PyTuple>()
            || val.is_instance_of::<PyMapping>()
        {
            let is_empty = is_empty_row(&val);
            return Ok(PyValueList {
                inner: val.unbind(),
                is_empty,
            });
        }

        let python_type_name = val.get_type().name()?;
        let python_type_name = python_type_name.extract::<&str>()?;

        Err(PyErr::new::<PyTypeError, _>(format!(
            "Invalid row type: got {}, expected Python tuple, list or Mapping (e.g. dict)",
            python_type_name
        )))
    }
}

fn is_empty_row(row: &Bound<'_, PyAny>) -> bool {
    if row.is_none() {
        return true;
    }

    row.len().map(|len| len == 0).unwrap_or(false)
}
