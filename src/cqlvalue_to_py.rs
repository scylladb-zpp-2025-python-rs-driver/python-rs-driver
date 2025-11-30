use pyo3::prelude::*;
use pyo3::types::{PyBool, PyBytes, PyDict, PyFloat, PyInt, PyList, PyString};
use scylla::value::CqlValue;

pub fn cql_value_to_py(py: Python<'_>, v: &CqlValue) -> PyResult<PyObject> {
    use CqlValue::*;

    let obj = match v {
        Ascii(s) | Text(s) => PyString::new(py, s).into_any(),

        Boolean(b) => PyBool::new(py, *b).to_owned().into_any(),

        Int(i) => PyInt::new(py, *i).into_any(),
        BigInt(i) => PyInt::new(py, *i).into_any(),
        SmallInt(i) => PyInt::new(py, *i).into_any(),
        TinyInt(i) => PyInt::new(py, *i).into_any(),

        Double(f) => PyFloat::new(py, *f).into_any(),
        Float(f) => PyFloat::new(py, *f as f64).into_any(),

        // Blob -> bytes
        Blob(bytes) => PyBytes::new(py, bytes).into_any(),

        // UUID
        Uuid(u) => {
            let uuid_mod = py.import("uuid")?;
            let uuid_cls = uuid_mod.getattr("UUID")?;
            uuid_cls.call1((u.to_string(),))?.into_any()
        }

        // Inet -> ipaddress.ip_address
        Inet(ip) => {
            let ipaddress_mod = py.import("ipaddress")?;
            let ip_address_cls = ipaddress_mod.getattr("ip_address")?;
            ip_address_cls.call1((ip.to_string(),))?.into_any()
        }

        // List / Set / Vector -> Python list
        List(values) | Set(values) | Vector(values) => {
            let py_list = PyList::empty(py);
            for inner in values {
                py_list.append(cql_value_to_py(py, inner)?)?;
            }
            py_list.into_any()
        }

        // Map -> dict
        Map(entries) => {
            let dict = PyDict::new(py);
            for (k, v) in entries {
                let py_k = cql_value_to_py(py, k)?;
                let py_v = cql_value_to_py(py, v)?;
                dict.set_item(py_k, py_v)?;
            }
            dict.into_any()
        }

        // UDT -> dict field_name -> value
        UserDefinedType { fields, .. } => {
            let dict = PyDict::new(py);
            for (name, opt) in fields {
                let val = match opt {
                    Some(inner) => cql_value_to_py(py, inner)?.into_any(),
                    None => py.None().into_any(),
                };
                dict.set_item(name, val)?;
            }
            dict.into_any()
        }

        other => {
            return Err(pyo3::exceptions::PyRuntimeError::new_err(format!(
                "Unsupported CqlValue variant in Python mapping: {other:?}"
            )));
        }
    };

    Ok(obj.into())
}
