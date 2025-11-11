use pyo3::prelude::{PyModule, PyModuleMethods};
use pyo3::{Bound, Py, PyAny, PyResult, Python, pyclass, pymodule};


#[pyclass(subclass)]
pub struct PyNativeType;

#[pyclass(extends=PyNativeType)]
pub struct Double;

#[pyclass(extends=PyNativeType)]
pub struct Text;

#[pyclass(extends=PyNativeType)]
pub struct Int;

#[pyclass(extends=PyNativeType)]
pub struct Float;

#[pyclass(extends=PyNativeType)]
pub struct Boolean;

#[pyclass(extends=PyNativeType)]
pub struct BigInt;

#[pyclass(subclass)]
pub struct PyCollectionType {
    #[pyo3(get)]
    pub frozen: bool,
}

#[pyclass(extends=PyCollectionType)]
pub struct Map {
    #[pyo3(get)]
    pub key_type: Py<PyAny>,
    #[pyo3(get)]
    pub value_type: Py<PyAny>,
}

#[pyclass(extends=PyCollectionType)]
pub struct Set {
    #[pyo3(get)]
    pub column_type: Py<PyAny>,
}

#[pyclass(extends=PyCollectionType)]
pub struct List {
    #[pyo3(get)]
    pub column_type: Py<PyAny>,
}

#[pyclass]
pub struct PyTuple {
    #[pyo3(get)]
    pub element_types: Vec<Py<PyAny>>,
}

#[pyclass]
pub struct PyUserDefinedType {
    #[pyo3(get)]
    pub name: String,
    #[pyo3(get)]
    pub frozen: bool,
    #[pyo3(get)]
    pub keyspace: String,
    #[pyo3(get)]
    pub field_types: Vec<(String, Py<PyAny>)>,
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

    Ok(())
}
