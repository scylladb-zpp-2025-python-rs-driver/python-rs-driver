use pyo3::exceptions::PyTypeError;
use pyo3::prelude::*;
use pyo3::types::{PyDict, PyList, PyTuple};
use pyo3::{Py, PyAny};
use scylla::_macro_internal::{ColumnType, RowSerializationContext, UdtTypeCheckErrorKind};
use scylla::_macro_internal::{RowWriter, SerializeRow, SerializeValue, WrittenCellProof};
use scylla::cluster::metadata::NativeType;
use scylla::frame::response::result::{CollectionType, UserDefinedType};
use scylla::serialize::SerializationError;
use scylla::serialize::value::{
    BuiltinSerializationError, BuiltinSerializationErrorKind, BuiltinTypeCheckError,
    BuiltinTypeCheckErrorKind,
};
use scylla::serialize::writers::CellWriter;
use scylla::value::CqlValue;
use std::ops::Deref;
use std::sync::Arc;
use thiserror::Error;

#[derive(Error, Debug)]
pub(crate) enum PythonDriverSerializationError {
    #[error("Python error")]
    PythonError(#[from] PyErr)
}

#[derive(Debug)]
pub(crate) struct PyAnyWrapper<'py>(pub(crate) Bound<'py, PyAny>);

impl<'py> Deref for PyAnyWrapper<'py> {
    type Target = Bound<'py, PyAny>;
    fn deref(&self) -> &Self::Target {
        &self.0
    }
}
static INT_COLUMNS: &[ColumnType<'static>] = &[
    ColumnType::Native(NativeType::Int),
    ColumnType::Native(NativeType::BigInt),
];
static FLOAT_COLUMNS: &[ColumnType<'static>] = &[ColumnType::Native(NativeType::Double)];
static STRING_COLUMNS: &[ColumnType<'static>] = &[ColumnType::Native(NativeType::Text)];
static BOOL_COLUMNS: &[ColumnType<'static>] = &[ColumnType::Native(NativeType::Boolean)];

impl<'py> PyAnyWrapper<'py> {
    fn map_type_to_expected(&self) -> Result<&'static [ColumnType<'static>], SerializationError> {
        let python_type_name = self
            .get_type()
            .name()
            .map_err(|e| SerializationError::new(PythonDriverSerializationError::PythonError(e)))?;

        let python_type_name: &str = python_type_name
            .extract()
            .map_err(|e| SerializationError::new(PythonDriverSerializationError::PythonError(e)))?;

        let columns = match python_type_name {
            "int" => INT_COLUMNS,
            "float" => FLOAT_COLUMNS,
            "string" => STRING_COLUMNS,
            "bool" => BOOL_COLUMNS,
            _ => unimplemented!(),
        };

        Ok(columns)
    }

    fn serialize_natives<'a>(
        &self,
        typ: &ColumnType,
        native_type: &NativeType,
        cell_writer: CellWriter<'a>,
    ) -> Result<WrittenCellProof<'a>, SerializationError> {
        match native_type {
            NativeType::Boolean => self.serialize_native::<bool>(typ, cell_writer),
            NativeType::BigInt => self.serialize_native::<i64>(typ, cell_writer),
            NativeType::Double => self.serialize_native::<f64>(typ, cell_writer),
            NativeType::Int => self.serialize_native::<i32>(typ, cell_writer),
            NativeType::Text => self.serialize_text(typ, cell_writer),
            _ => unimplemented!("other native types not supported yet"),
        }
    }

    // In future PyO3 release FromPyObjectBound can be used instead of FromPyObject.
    fn serialize_native<'a, T>(
        &self,
        typ: &ColumnType,
        cell_writer: CellWriter<'a>,
    ) -> Result<WrittenCellProof<'a>, SerializationError>
    where
        T: for<'b> pyo3::FromPyObject<'b> + SerializeValue,
    {
        let value = match self.extract::<T>() {
            Ok(val) => val,
            Err(_) => {
                let expected = self.map_type_to_expected()?;
                return Err(SerializationError::new(mk_typck_err::<PyList>(
                    typ,
                    BuiltinTypeCheckErrorKind::MismatchedType { expected },
                )));
            }
        };

        value.serialize(typ, cell_writer)
    }

    fn serialize_text<'a>(
        &self,
        typ: &ColumnType,
        cell_writer: CellWriter<'a>,
    ) -> Result<WrittenCellProof<'a>, SerializationError> {
        let value = match self.extract::<&str>() {
            Ok(val) => val,
            Err(_) => {
                let expected = self.map_type_to_expected()?;
                return Err(SerializationError::new(mk_typck_err::<PyList>(
                    typ,
                    BuiltinTypeCheckErrorKind::MismatchedType { expected },
                )));
            }
        };
        value.serialize(typ, cell_writer)
    }

    fn serialize_types<'a>(
        &self,
        typ: &ColumnType,
        cell_writer: CellWriter<'a>,
    ) -> Result<WrittenCellProof<'a>, SerializationError> {
        match typ {
            ColumnType::Native(native_type) => {
                self.serialize_natives(typ, native_type, cell_writer)
            }
            ColumnType::Collection {
                frozen: _,
                typ: collection_typ,
            } => match collection_typ {
                CollectionType::List(element_type) => {
                    let list = match PyListWrapper::new(self, typ) {
                        Ok(list) => list,
                        Err(_) => {
                            let expected = self.map_type_to_expected()?;
                            return Err(SerializationError::new(mk_typck_err::<PyList>(
                                typ,
                                BuiltinTypeCheckErrorKind::MismatchedType { expected },
                            )));
                        }
                    };
                    list.serialize(element_type, cell_writer)
                }
                _ => unimplemented!("other collection types not supported yet"),
            },
            // Supports UDTs passed as Python dicts.
            // For Python dataclass instances, convert to dict first (e.g., using dataclasses.asdict()).
            ColumnType::UserDefinedType { definition, .. } => {
                let dict = match PyUdtWrapper::new(self, definition) {
                    Ok(dict) => dict,
                    Err(_) => {
                        let expected = self.map_type_to_expected()?;
                        return Err(SerializationError::new(mk_typck_err::<PyList>(
                            typ,
                            BuiltinTypeCheckErrorKind::MismatchedType { expected },
                        )));
                    }
                };
                dict.serialize(typ, cell_writer)
            }
            _ => unimplemented!("other types not supported yet"),
        }
    }
}

impl<'py> SerializeValue for PyAnyWrapper<'py> {
    fn serialize<'a>(
        &self,
        typ: &ColumnType,
        cell_writer: CellWriter<'a>,
    ) -> Result<WrittenCellProof<'a>, SerializationError> {
        if self.is_none() {
            return Ok(cell_writer.set_null());
        }

        self.serialize_types(typ, cell_writer)
    }
}

pub(crate) struct PyAnyWrapperRow(pub(crate) Py<PyAny>);

impl Deref for PyAnyWrapperRow {
    type Target = Py<PyAny>;
    fn deref(&self) -> &Self::Target {
        &self.0
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

            if let Ok(py_tuple) = val.downcast::<PyTuple>() {
                if py_tuple.len() != ctx.columns().len() {
                    return Err(SerializationError::new(mk_typck_err_row::<PyTuple>(
                        scylla::serialize::row::BuiltinTypeCheckErrorKind::WrongColumnCount {
                            rust_cols: py_tuple.len(),
                            cql_cols: ctx.columns().len(),
                        },
                    )));
                }

                for (col, val) in ctx.columns().iter().zip(py_tuple.iter()) {
                    let wrapper = PyAnyWrapper(val);
                    let sub_writer = row_writer.make_cell_writer();
                    SerializeValue::serialize(&wrapper, col.typ(), sub_writer)?;
                }
                return Ok(());
            }

            if let Ok(py_list) = val.downcast::<PyList>() {
                if py_list.len() != ctx.columns().len() {
                    return Err(SerializationError::new(mk_typck_err_row::<PyList>(
                        scylla::serialize::row::BuiltinTypeCheckErrorKind::WrongColumnCount {
                            rust_cols: py_list.len(),
                            cql_cols: ctx.columns().len(),
                        },
                    )));
                }

                for (col, val) in ctx.columns().iter().zip(py_list.iter()) {
                    let wrapper = PyAnyWrapper(val);
                    let sub_writer = row_writer.make_cell_writer();
                    SerializeValue::serialize(&wrapper, col.typ(), sub_writer)?;
                }
                return Ok(());
            }

            Err(SerializationError::new(PyTypeError::new_err(
                "expected Python tuple or list",
            )))
        })
    }

    fn is_empty(&self) -> bool {
        Python::with_gil(|py| self.is_none(py))
    }
}

#[derive(Debug)]
pub(crate) struct PyListWrapper<'py, 'a> {
    pub(crate) inner: Bound<'py, PyList>,
    pub(crate) list_type: &'a ColumnType<'a>,
}

impl<'py> Deref for PyListWrapper<'py, '_> {
    type Target = Bound<'py, PyList>;
    fn deref(&self) -> &Self::Target {
        &self.inner
    }
}

impl<'py, 'a> PyListWrapper<'py, 'a> {
    fn new(value: &PyAnyWrapper<'py>, list_type: &'a ColumnType<'a>) -> PyResult<Self> {
        let list: &Bound<PyList> = value.downcast::<PyList>()?;
        Ok(PyListWrapper {
            inner: Bound::clone(list),
            list_type,
        })
    }
}

impl<'py> SerializeValue for PyListWrapper<'py, '_> {
    fn serialize<'b>(
        &self,
        _typ: &ColumnType,
        cell_writer: CellWriter<'b>,
    ) -> Result<WrittenCellProof<'b>, SerializationError> {
        let items: Vec<PyAnyWrapper> = self.iter().map(PyAnyWrapper).collect::<Vec<_>>();

        SerializeValue::serialize(&items, self.list_type, cell_writer)
    }
}

pub(crate) struct PyUdtWrapper<'py, 'a> {
    pub(crate) inner: Bound<'py, PyDict>,
    pub(crate) definition: &'a Arc<UserDefinedType<'a>>,
}

impl<'py> Deref for PyUdtWrapper<'py, '_> {
    type Target = Bound<'py, PyDict>;
    fn deref(&self) -> &Self::Target {
        &self.inner
    }
}

impl<'py, 'a> PyUdtWrapper<'py, 'a> {
    fn new(value: &PyAnyWrapper<'py>, definition: &'a Arc<UserDefinedType<'a>>) -> PyResult<Self> {
        let dict: &Bound<PyDict> = value.downcast::<PyDict>()?;
        Ok(PyUdtWrapper {
            inner: dict.clone(),
            definition,
        })
    }
}

impl<'py> SerializeValue for PyUdtWrapper<'py, '_> {
    fn serialize<'a>(
        &self,
        typ: &ColumnType,
        cell_writer: CellWriter<'a>,
    ) -> Result<WrittenCellProof<'a>, SerializationError> {
        let mut builder = cell_writer.into_value_builder();

        for (field_name, field_type) in &self.definition.field_types {
            let item: Bound<PyAny> = self
                .inner
                .get_item(field_name)
                .map_err(|e| {
                    SerializationError::new(PythonDriverSerializationError::PythonError(e))
                })?
                .ok_or_else(|| {
                    mk_typck_err::<PyDict>(
                        typ,
                        UdtTypeCheckErrorKind::ValueMissingForUdtField {
                            field_name: field_name.to_string(),
                        },
                    )
                })?;

            PyAnyWrapper(item).serialize_types(field_type, builder.make_sub_writer())?;
        }

        builder
            .finish()
            .map_err(|_| mk_ser_err::<CqlValue>(typ, BuiltinSerializationErrorKind::SizeOverflow))
    }
}

fn mk_typck_err<T: ?Sized>(
    got: &ColumnType,
    kind: impl Into<BuiltinTypeCheckErrorKind>,
) -> SerializationError {
    mk_typck_err_named(std::any::type_name::<T>(), got, kind)
}

fn mk_typck_err_named(
    name: &'static str,
    got: &ColumnType,
    kind: impl Into<BuiltinTypeCheckErrorKind>,
) -> SerializationError {
    SerializationError::new(BuiltinTypeCheckError {
        rust_name: name,
        got: got.clone().into_owned(),
        kind: kind.into(),
    })
}

pub fn mk_typck_err_row<T>(
    kind: impl Into<scylla::serialize::row::BuiltinTypeCheckErrorKind>,
) -> SerializationError {
    mk_typck_err_named_row(std::any::type_name::<T>(), kind)
}

fn mk_typck_err_named_row(
    name: &'static str,
    kind: impl Into<scylla::serialize::row::BuiltinTypeCheckErrorKind>,
) -> SerializationError {
    SerializationError::new(scylla::serialize::row::BuiltinTypeCheckError {
        rust_name: name,
        kind: kind.into(),
    })
}

fn mk_ser_err<T: ?Sized>(
    got: &ColumnType,
    kind: impl Into<BuiltinSerializationErrorKind>,
) -> SerializationError {
    mk_ser_err_named(std::any::type_name::<T>(), got, kind)
}

fn mk_ser_err_named(
    name: &'static str,
    got: &ColumnType,
    kind: impl Into<BuiltinSerializationErrorKind>,
) -> SerializationError {
    SerializationError::new(BuiltinSerializationError {
        rust_name: name,
        got: got.clone().into_owned(),
        kind: kind.into(),
    })
}
