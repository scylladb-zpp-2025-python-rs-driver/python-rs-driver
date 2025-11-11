use crate::column_type::*;
use crate::statements::PyPreparedStatement;
use pyo3::exceptions::PyValueError;
use pyo3::prelude::*;
use pyo3::{Bound, IntoPyObjectExt, PyResult, Python, pyclass, pymethods, pymodule};
use scylla::_macro_internal::{CellWriter, ColumnType, RowSerializationContext};
use scylla::cluster::metadata::{CollectionType, NativeType};
use scylla::serialize::SerializationError;
use scylla::serialize::writers::CellOverflowError;
use scylla::statement::prepared::PreparedStatement;
use scylla_cql::serialize::row::SerializedValues;
use std::sync::{Arc, Mutex};

pub enum PySerializationError {
    Serialization(SerializationError),
    Overflow(CellOverflowError),
}

impl From<PySerializationError> for PyErr {
    fn from(err: PySerializationError) -> Self {
        PyValueError::new_err(match err {
            PySerializationError::Serialization(e) => e.to_string(),
            PySerializationError::Overflow(e) => e.to_string(),
        })
    }
}

impl From<SerializationError> for PySerializationError {
    fn from(err: SerializationError) -> Self {
        PySerializationError::Serialization(err)
    }
}

impl From<CellOverflowError> for PySerializationError {
    fn from(err: CellOverflowError) -> Self {
        PySerializationError::Overflow(err)
    }
}

#[pyclass]
pub struct PyRowSerializationContext {
    prepared_statement: Arc<PreparedStatement>,
}

#[pymethods]
impl PyRowSerializationContext {
    #[staticmethod]
    pub fn from_prepared(prepared_statement: PyRef<'_, PyPreparedStatement>) -> Self {
        Self {
            prepared_statement: Arc::clone(&prepared_statement._inner),
        }
    }

    pub fn column_count(&self) -> usize {
        RowSerializationContext::from_specs(
            self.prepared_statement.get_variable_col_specs().as_slice(),
        )
        .columns()
        .len()
    }

    pub fn get_columns(&self) -> PyResult<Vec<Py<PyAny>>> {
        let context = RowSerializationContext::from_specs(
            self.prepared_statement.get_variable_col_specs().as_slice(),
        );
        let mut columns = Vec::new();

        Python::with_gil(|py| {
            for col_spec in context.columns() {
                let typ = extract_type(py, col_spec.typ())?;

                columns.push(typ);
            }

            Ok(columns)
        })
    }
}

fn extract_type(py: Python<'_>, column_type: &ColumnType) -> PyResult<Py<PyAny>> {
    match column_type {
        ColumnType::Native(native_type) => match native_type {
            NativeType::Int => Py::new(
                py,
                PyClassInitializer::from(PyNativeType {}).add_subclass(Int {}),
            )?
            .into_py_any(py),
            NativeType::Text => Py::new(
                py,
                PyClassInitializer::from(PyNativeType {}).add_subclass(Text {}),
            )?
            .into_py_any(py),
            NativeType::Float => Py::new(
                py,
                PyClassInitializer::from(PyNativeType {}).add_subclass(Float {}),
            )?
            .into_py_any(py),
            NativeType::Double => Py::new(
                py,
                PyClassInitializer::from(PyNativeType {}).add_subclass(Double {}),
            )?
            .into_py_any(py),
            NativeType::Boolean => Py::new(
                py,
                PyClassInitializer::from(PyNativeType {}).add_subclass(Boolean {}),
            )?
            .into_py_any(py),
            NativeType::BigInt => Py::new(
                py,
                PyClassInitializer::from(PyNativeType {}).add_subclass(BigInt {}),
            )?
            .into_py_any(py),
            _ => unimplemented!(),
        },
        ColumnType::Collection { frozen, typ } => match typ {
            CollectionType::List(element_type) => {
                let column_type = extract_type(py, element_type)?;
                Py::new(
                    py,
                    PyClassInitializer::from(PyCollectionType { frozen: *frozen })
                        .add_subclass(List { column_type }),
                )?
                .into_py_any(py)
            }
            CollectionType::Set(element_type) => {
                let column_type = extract_type(py, element_type)?;
                Py::new(
                    py,
                    PyClassInitializer::from(PyCollectionType { frozen: *frozen })
                        .add_subclass(Set { column_type }),
                )?
                .into_py_any(py)
            }
            CollectionType::Map(key_type, value_type) => {
                let key_type = extract_type(py, key_type)?;
                let value_type = extract_type(py, value_type)?;
                Py::new(
                    py,
                    PyClassInitializer::from(PyCollectionType { frozen: *frozen }).add_subclass(
                        Map {
                            key_type,
                            value_type,
                        },
                    ),
                )?
                .into_py_any(py)
            }
            _ => unimplemented!(),
        },
        ColumnType::UserDefinedType { frozen, definition } => {
            let mut fields = Vec::new();

            for (field_name, field_type) in &definition.field_types {
                let field_type = extract_type(py, field_type)?;
                fields.push((field_name.to_string(), field_type));
            }

            Py::new(
                py,
                PyClassInitializer::from(PyUserDefinedType {
                    name: definition.name.to_string(),
                    frozen: *frozen,
                    keyspace: definition.keyspace.to_string(),
                    field_types: fields,
                }),
            )?
            .into_py_any(py)
        }
        ColumnType::Tuple(element_list) => {
            let mut element_types = Vec::new();
            for typ in element_list {
                let element_type = extract_type(py, typ)?;
                element_types.push(element_type);
            }
            Py::new(py, PyClassInitializer::from(PyTuple { element_types }))?.into_py_any(py)
        }
        _ => unimplemented!(),
    }
}

#[pyclass]
#[derive(Debug, Clone)]
pub struct SerializationBuffer {
    buff: Arc<Mutex<Vec<u8>>>,
    element_count: u16,
}

#[pymethods]
impl SerializationBuffer {
    #[new]
    pub fn new() -> Self {
        Self {
            buff: Arc::new(Mutex::new(Vec::new())),
            element_count: 0,
        }
    }

    pub fn len(&self) -> usize {
        self.buff.lock().unwrap().len()
    }

    pub fn set_element_count(&mut self, element_count: u16) {
        self.element_count = element_count;
    }
}

impl SerializationBuffer {
    pub fn get_serialized_values(&self) -> SerializedValues {
        let buff = self.buff.lock().unwrap();
        SerializedValues {
            serialized_values: buff.clone(),
            element_count: self.element_count,
        }
    }
}

impl Default for SerializationBuffer {
    fn default() -> Self {
        Self::new()
    }
}

#[pyclass]
pub struct PyRowWriter {
    value_count: usize,
    buff: Arc<Mutex<Vec<u8>>>,
}

#[pymethods]
impl PyRowWriter {
    #[new]
    pub fn new(buffer: PyRef<'_, SerializationBuffer>) -> Self {
        Self {
            value_count: 0,
            buff: Arc::clone(&buffer.buff),
        }
    }

    pub fn make_cell_writer(&mut self) -> PyCellWriter {
        self.value_count += 1;
        PyCellWriter {
            buff: Arc::clone(&self.buff),
            write_size: true,
        }
    }

    pub fn value_count(&self) -> usize {
        self.value_count
    }
}

#[pyclass]
pub struct PyCellWriter {
    buff: Arc<Mutex<Vec<u8>>>,
    write_size: bool,
}

#[pymethods]
impl PyCellWriter {
    #[new]
    pub fn new(buffer: PyRef<'_, SerializationBuffer>) -> Self {
        Self {
            buff: Arc::clone(&buffer.buff),
            write_size: true,
        }
    }

    #[staticmethod]
    pub fn new_without_size(buffer: PyRef<'_, SerializationBuffer>) -> Self {
        Self {
            buff: Arc::clone(&buffer.buff),
            write_size: false,
        }
    }

    pub fn set_value(&mut self, value: &[u8]) -> Result<(), PySerializationError> {
        let mut buff = self.buff.lock().unwrap();
        let mut_writer = if self.write_size {
            CellWriter::new(&mut buff)
        } else {
            CellWriter::new_without_size(&mut buff)
        };

        mut_writer.set_value(value)?;
        Ok(())
    }

    pub fn set_null(&mut self) {
        let mut buff = self.buff.lock().unwrap();
        let mut_writer = if self.write_size {
            CellWriter::new(&mut buff)
        } else {
            CellWriter::new_without_size(&mut buff)
        };
        mut_writer.set_null();
    }

    pub fn set_unset(&mut self) {
        let mut buff = self.buff.lock().unwrap();

        let mut_writer = if self.write_size {
            CellWriter::new(&mut buff)
        } else {
            CellWriter::new_without_size(&mut buff)
        };
        mut_writer.set_unset();
    }

    pub fn into_value_builder(&self) -> PyCellValueBuilder {
        PyCellValueBuilder::new(self.buff.clone(), self.write_size)
    }
}

#[pyclass]
pub struct PyCellValueBuilder {
    buff: Arc<Mutex<Vec<u8>>>,
    starting_pos: usize,
    write_size: bool,
}

#[pymethods]
impl PyCellValueBuilder {
    pub fn append_bytes(&mut self, bytes: &[u8]) {
        let mut buff = self.buff.lock().unwrap();

        buff.extend_from_slice(bytes);
    }

    pub fn make_sub_writer(&mut self) -> PyCellWriter {
        PyCellWriter {
            buff: Arc::clone(&self.buff),
            write_size: true,
        }
    }

    pub fn make_sub_writer_without_size(&mut self) -> PyCellWriter {
        PyCellWriter {
            buff: Arc::clone(&self.buff),
            write_size: false,
        }
    }

    pub fn finish(&self) -> Result<(), PySerializationError> {
        let mut buff = self.buff.lock().unwrap();
        if self.write_size {
            let value_len: i32 = (buff.len() - self.starting_pos - 4)
                .try_into()
                .map_err(|_| PySerializationError::Overflow(CellOverflowError))?;
            buff[self.starting_pos..self.starting_pos + 4]
                .copy_from_slice(&value_len.to_be_bytes());
        }
        Ok(())
    }
}

impl PyCellValueBuilder {
    fn new(buffer: Arc<Mutex<Vec<u8>>>, write_size: bool) -> Self {
        let mut buff = buffer.lock().unwrap();

        let starting_pos = buff.len();
        if write_size {
            buff.extend_from_slice(&(-3i32).to_be_bytes());
        }
        Self {
            buff: Arc::clone(&buffer),
            starting_pos,
            write_size,
        }
    }
}

#[pymodule]
pub(crate) fn writers(_py: Python<'_>, module: &Bound<'_, PyModule>) -> PyResult<()> {
    module.add_class::<PyRowWriter>()?;
    module.add_class::<PyCellWriter>()?;
    module.add_class::<SerializationBuffer>()?;
    module.add_class::<PyRowSerializationContext>()?;
    module.add_class::<PyCellValueBuilder>()?;

    Ok(())
}
