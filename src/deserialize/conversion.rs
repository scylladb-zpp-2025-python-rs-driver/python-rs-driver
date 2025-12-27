use bigdecimal_04::Zero;
use pyo3::sync::GILOnceCell;
use pyo3::types::{PyAnyMethods, PyDict, PyInt, PyTuple, PyType};
use pyo3::{Bound, IntoPyObject, Py, PyAny, PyErr, PyResult, Python, ffi};
use scylla_cql::value::{CqlDecimalBorrowed, CqlDuration, CqlVarintBorrowed};

static DECIMAL_CLS: GILOnceCell<Py<PyType>> = GILOnceCell::new();
static INVALID_OPERATION_CLS: GILOnceCell<Py<PyType>> = GILOnceCell::new();
static RELATIVEDELTA_CLS: GILOnceCell<Py<PyType>> = GILOnceCell::new();

fn get_decimal_cls(py: Python<'_>) -> PyResult<&Bound<'_, PyType>> {
    DECIMAL_CLS.import(py, "decimal", "Decimal")
}

fn get_relative_delta_cls(py: Python<'_>) -> PyResult<&Bound<'_, PyType>> {
    RELATIVEDELTA_CLS.import(py, "dateutil.relativedelta", "relativedelta")
}

fn get_invalid_operation_error_cls(py: Python<'_>) -> PyResult<&Bound<'_, PyType>> {
    INVALID_OPERATION_CLS.import(py, "decimal", "InvalidOperation")
}

pub(crate) struct CqlVarintWrapper<'b> {
    val: CqlVarintBorrowed<'b>,
}

impl<'b> From<CqlVarintBorrowed<'b>> for CqlVarintWrapper<'b> {
    fn from(val: CqlVarintBorrowed<'b>) -> Self {
        Self { val }
    }
}

impl<'py> IntoPyObject<'py> for CqlVarintWrapper<'_> {
    type Target = PyInt;
    type Output = Bound<'py, Self::Target>;
    type Error = PyErr;

    fn into_pyobject(self, py: Python<'py>) -> Result<Self::Output, Self::Error> {
        let bytes = self.val.as_signed_bytes_be_slice();
        unsafe {
            let val = ffi::_PyLong_FromByteArray(bytes.as_ptr(), bytes.len(), 0, 1);

            Ok(Bound::from_owned_ptr(py, val).downcast_into()?)
        }
    }
}

pub(crate) struct CqlDecimalWrapper<'b> {
    val: CqlDecimalBorrowed<'b>,
}

impl<'b> From<CqlDecimalBorrowed<'b>> for CqlDecimalWrapper<'b> {
    fn from(val: CqlDecimalBorrowed<'b>) -> Self {
        Self { val }
    }
}

// NOTE:
// This implementation uses a straightforward positional-accumulation algorithm
// (evaluating the number as Σ digit × 256^i using base-10 big-integer arithmetic).
//
// Time complexity is approximately O(n²) in the number of input bytes:
// each new base-256 digit triggers big-integer scalar multiplication and addition.
//
// For large inputs, this is not asymptotically optimal. Crates such as
// `num_bigint::BigUint` use more efficient base-conversion strategies
// (e.g. division-based conversion, chunked processing) to improve scaling.
//
// Applying similar optimizations here would require non-trivial refactoring
// It would be better addressed in a separate issue focused specifically on performance.
impl<'py> IntoPyObject<'py> for CqlDecimalWrapper<'_> {
    type Target = PyAny;

    type Output = Bound<'py, Self::Target>;

    type Error = PyErr;

    fn into_pyobject(self, py: Python<'py>) -> Result<Self::Output, Self::Error> {
        let cls = get_decimal_cls(py)?;

        let (bytes, scale) = self.val.as_signed_be_bytes_slice_and_exponent();

        let exponent = scale.checked_neg().ok_or_else(|| {
            get_invalid_operation_error_cls(py)
                .map_or_else(|err| err, |cls| PyErr::from_type(cls.clone(), ()))
        })?;

        let (signed, magnitude) = signed_be_bytes_to_magnitude(bytes);
        let digits = PyTuple::new(py, convert_base256_to_base10(&magnitude))?;

        cls.call1(((signed, digits, exponent),))
    }
}

pub(crate) struct CqlDurationWrapper {
    val: CqlDuration,
}

impl From<CqlDuration> for CqlDurationWrapper {
    fn from(val: CqlDuration) -> Self {
        Self { val }
    }
}

impl<'py> IntoPyObject<'py> for CqlDurationWrapper {
    type Target = PyAny;
    type Output = Bound<'py, Self::Target>;
    type Error = PyErr;

    fn into_pyobject(self, py: Python<'py>) -> Result<Self::Output, Self::Error> {
        let cls = get_relative_delta_cls(py)?;
        let duration = &self.val;
        let kwargs = PyDict::new(py);
        kwargs.set_item("months", duration.months)?;
        kwargs.set_item("days", duration.days)?;
        kwargs.set_item("microseconds", duration.nanoseconds / 1000)?;

        cls.call((), Some(&kwargs))
    }
}

fn signed_be_bytes_to_magnitude(bytes: &[u8]) -> (bool, Vec<u8>) {
    if bytes.is_empty() {
        return (false, vec![0]);
    }

    let is_negative = (bytes[0] & 0x80) != 0;
    let mut mag: Vec<u8> = bytes.iter().rev().copied().collect();

    if is_negative {
        twos_complement_le(&mut mag);
    }

    while mag.len() > 1 && mag.last().is_some_and(|&b| b == 0) {
        mag.pop();
    }

    (is_negative, mag)
}

fn twos_complement_le(digits: &mut [u8]) {
    let mut carry = true;
    for d in digits.iter_mut() {
        *d = !*d;
        if carry {
            *d = d.wrapping_add(1);
            carry = d.is_zero();
        }
    }
}

// Convert a number from base 256 to base 10.
// Input and output are little-endian (least-significant digit first).
fn convert_base256_to_base10(input: &[u8]) -> Vec<u8> {
    // Result digits in base 10
    let mut output: Vec<u8> = Vec::new();

    // Current power of 256, stored in base 10
    // Starts at 1 (i.e. 256^0)
    let mut base: Vec<u8> = vec![1];

    for &digit in input {
        if digit != 0 {
            // temp = base * digit
            let mut temp = base.clone();
            mul_scalar_base10(&mut temp, digit as u32);

            // output += temp
            add_base10(&mut output, &temp);
        }

        // base *= 256 for the next byte
        mul_scalar_base10(&mut base, 256);
    }

    output.reverse();
    output
}

fn mul_scalar_base10(num: &mut Vec<u8>, mul: u32) {
    let mut carry = 0u32;

    for d in num.iter_mut() {
        let v = (*d as u32) * mul + carry;
        *d = (v % 10) as u8;
        carry = v / 10;
    }

    while carry > 0 {
        num.push((carry % 10) as u8);
        carry /= 10;
    }
}

fn add_base10(dst: &mut Vec<u8>, src: &[u8]) {
    let mut carry = 0u32;
    let max_len = dst.len().max(src.len());

    if dst.len() < max_len {
        dst.resize(max_len, 0);
    }

    for (i, d) in dst.iter_mut().enumerate().take(max_len) {
        let a = *d as u32;
        let b = src.get(i).copied().unwrap_or(0) as u32;

        let v = a + b + carry;
        *d = (v % 10) as u8;
        carry = v / 10;
    }

    while carry > 0 {
        dst.push((carry % 10) as u8);
        carry /= 10;
    }
}
