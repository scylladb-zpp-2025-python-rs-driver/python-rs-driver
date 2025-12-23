use crate::serialize::value::{PyAnyWrapper, PythonDriverSerializationError};
use pyo3::prelude::*;
use pyo3::types::{PyDict, PyList, PyTuple};
use pyo3::{Bound, Py, PyAny, Python};
use scylla::errors::SerializationError;
use scylla::frame::response::result::ColumnSpec;
use scylla::serialize::row::{BuiltinTypeCheckError, BuiltinTypeCheckErrorKind};
use scylla::serialize::row::{RowSerializationContext, SerializeRow};
use scylla::serialize::value::SerializeValue;
use scylla::serialize::writers::{RowWriter, WrittenCellProof};
use std::ops::Deref;

fn length_equality_check(
    row_len: usize,
    cols_len: usize,
    name: &'static str,
) -> Result<(), SerializationError> {
    if row_len != cols_len {
        return Err(SerializationError::new(mk_typck_err_named_row(
            name,
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
    let wrapper = PyAnyWrapper(val);
    let sub_writer = row_writer.make_cell_writer();
    SerializeValue::serialize(&wrapper, col.typ(), sub_writer)
}

pub(crate) struct PyListWrapperRow(pub(crate) Py<PyList>);

impl Deref for PyListWrapperRow {
    type Target = Py<PyList>;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl SerializeRow for PyListWrapperRow {
    fn serialize(
        &self,
        ctx: &RowSerializationContext<'_>,
        row_writer: &mut RowWriter,
    ) -> Result<(), SerializationError> {
        Python::with_gil(|py| {
            let row = self.bind(py);

            length_equality_check(
                row.len(),
                ctx.columns().len(),
                std::any::type_name::<PyList>(),
            )?;

            for (col, val) in ctx.columns().iter().zip(row.iter()) {
                serialize_element(col, &val, row_writer)?;
            }

            Ok(())
        })
    }

    fn is_empty(&self) -> bool {
        Python::with_gil(|py| {
            let row = self.bind(py);
            row.is_empty()
        })
    }
}

pub(crate) struct PyTupleWrapperRow(pub(crate) Py<PyTuple>);

impl Deref for PyTupleWrapperRow {
    type Target = Py<PyTuple>;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl SerializeRow for PyTupleWrapperRow {
    fn serialize(
        &self,
        ctx: &RowSerializationContext<'_>,
        row_writer: &mut RowWriter,
    ) -> Result<(), SerializationError> {
        Python::with_gil(|py| {
            let row = self.bind(py);

            length_equality_check(
                row.len(),
                ctx.columns().len(),
                std::any::type_name::<PyTuple>(),
            )?;
            for (col, val) in ctx.columns().iter().zip(row.iter()) {
                serialize_element(col, &val, row_writer)?;
            }
            Ok(())
        })
    }

    fn is_empty(&self) -> bool {
        Python::with_gil(|py| {
            let row = self.bind(py);
            row.is_empty()
        })
    }
}

pub(crate) struct PyDictWrapperRow(pub(crate) Py<PyDict>);

impl Deref for PyDictWrapperRow {
    type Target = Py<PyDict>;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl SerializeRow for PyDictWrapperRow {
    fn serialize(
        &self,
        ctx: &RowSerializationContext<'_>,
        row_writer: &mut RowWriter,
    ) -> Result<(), SerializationError> {
        Python::with_gil(|py| {
            let row = self.bind(py);

            length_equality_check(
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
                            BuiltinTypeCheckErrorKind::ValueMissingForColumn {
                                name: col.name().into(),
                            },
                        ))
                    })?;
                serialize_element(col, &item, row_writer)?;
            }

            Ok(())
        })
    }

    fn is_empty(&self) -> bool {
        Python::with_gil(|py| {
            let row = self.bind(py);
            row.is_empty()
        })
    }
}

pub(crate) fn mk_typck_err_row<T>(
    kind: impl Into<BuiltinTypeCheckErrorKind>,
) -> SerializationError {
    mk_typck_err_named_row(std::any::type_name::<T>(), kind)
}

pub(crate) fn mk_typck_err_named_row(
    name: &'static str,
    kind: impl Into<BuiltinTypeCheckErrorKind>,
) -> SerializationError {
    SerializationError::new(BuiltinTypeCheckError {
        rust_name: name,
        kind: kind.into(),
    })
}
