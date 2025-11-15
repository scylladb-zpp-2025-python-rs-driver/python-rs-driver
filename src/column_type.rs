use pyo3::prelude::{PyModule, PyModuleMethods};
use pyo3::{Bound, Py, PyClassInitializer, PyResult, Python, pyclass, pymodule};

#[pyclass(subclass, extends=PyColumnType)]
pub struct PyNativeType;

impl PyNativeType {
    pub fn new() -> PyClassInitializer<Self> {
        PyClassInitializer::from(PyColumnType::new()).add_subclass(PyNativeType {})
    }
}

#[pyclass(extends=PyNativeType)]
pub struct Double;

impl Double {
    pub fn new() -> PyClassInitializer<Self> {
        PyNativeType::new().add_subclass(Double {})
    }
}

#[pyclass(extends=PyNativeType)]
pub struct Text;

impl Text {
    pub fn new() -> PyClassInitializer<Self> {
        PyNativeType::new().add_subclass(Text {})
    }
}
#[pyclass(extends=PyNativeType)]
pub struct Int;

impl Int {
    pub fn new() -> PyClassInitializer<Self> {
        PyNativeType::new().add_subclass(Int {})
    }
}

#[pyclass(extends=PyNativeType)]
pub struct Float;

impl Float {
    pub fn new() -> PyClassInitializer<Self> {
        PyNativeType::new().add_subclass(Float {})
    }
}

#[pyclass(extends=PyNativeType)]
pub struct Boolean;

impl Boolean {
    pub fn new() -> PyClassInitializer<Self> {
        PyNativeType::new().add_subclass(Boolean {})
    }
}

#[pyclass(extends=PyNativeType)]
pub struct BigInt;

impl BigInt {
    pub fn new() -> PyClassInitializer<Self> {
        PyNativeType::new().add_subclass(BigInt {})
    }
}

#[pyclass(subclass, extends=PyColumnType)]
pub struct PyCollectionType {
    #[pyo3(get)]
    pub frozen: bool,
}

impl PyCollectionType {
    pub fn new(frozen: bool) -> PyClassInitializer<Self> {
        PyClassInitializer::from(PyColumnType::new()).add_subclass(PyCollectionType { frozen })
    }
}

#[pyclass(extends=PyCollectionType)]
pub struct Map {
    #[pyo3(get)]
    pub key_type: Py<PyColumnType>,
    #[pyo3(get)]
    pub value_type: Py<PyColumnType>,
}

impl Map {
    pub fn new(
        frozen: bool,
        key_type: Py<PyColumnType>,
        value_type: Py<PyColumnType>,
    ) -> PyClassInitializer<Self> {
        PyCollectionType::new(frozen).add_subclass(Map {
            key_type,
            value_type,
        })
    }
}

#[pyclass(extends=PyCollectionType)]
pub struct Set {
    #[pyo3(get)]
    pub column_type: Py<PyColumnType>,
}

impl Set {
    pub fn new(frozen: bool, column_type: Py<PyColumnType>) -> PyClassInitializer<Self> {
        PyCollectionType::new(frozen).add_subclass(Set { column_type })
    }
}

#[pyclass(extends=PyCollectionType)]
pub struct List {
    #[pyo3(get)]
    pub column_type: Py<PyColumnType>,
}

impl List {
    pub fn new(frozen: bool, column_type: Py<PyColumnType>) -> PyClassInitializer<Self> {
        PyCollectionType::new(frozen).add_subclass(List { column_type })
    }
}

#[pyclass(extends=PyColumnType)]
pub struct PyTuple {
    #[pyo3(get)]
    pub element_types: Vec<Py<PyColumnType>>,
}
impl PyTuple {
    pub fn new(element_types: Vec<Py<PyColumnType>>) -> PyClassInitializer<Self> {
        PyClassInitializer::from(PyColumnType::new()).add_subclass(PyTuple { element_types })
    }
}

#[pyclass(extends=PyColumnType)]
pub struct PyUserDefinedType {
    #[pyo3(get)]
    pub name: String,
    #[pyo3(get)]
    pub frozen: bool,
    #[pyo3(get)]
    pub keyspace: String,
    #[pyo3(get)]
    pub field_types: Vec<(String, Py<PyColumnType>)>,
}

impl PyUserDefinedType {
    pub fn new(
        name: String,
        frozen: bool,
        keyspace: String,
        field_types: Vec<(String, Py<PyColumnType>)>,
    ) -> PyClassInitializer<Self> {
        PyClassInitializer::from(PyColumnType::new()).add_subclass(PyUserDefinedType {
            name,
            frozen,
            keyspace,
            field_types,
        })
    }
}
#[pyclass(subclass)]
pub struct PyColumnType {}

impl PyColumnType {
    fn new() -> Self {
        PyColumnType {}
    }
}

#[pymodule]
pub(crate) fn column_type(_py: Python<'_>, m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_class::<PyNativeType>()?;
    m.add_class::<Int>()?;
    m.add_class::<Float>()?;
    m.add_class::<Double>()?;
    m.add_class::<Text>()?;
    m.add_class::<Boolean>()?;
    m.add_class::<BigInt>()?;

    m.add_class::<PyCollectionType>()?;
    m.add_class::<List>()?;
    m.add_class::<Set>()?;
    m.add_class::<Map>()?;

    m.add_class::<PyTuple>()?;
    m.add_class::<PyUserDefinedType>()?;

    m.add_class::<PyColumnType>()?;

    Ok(())
}
