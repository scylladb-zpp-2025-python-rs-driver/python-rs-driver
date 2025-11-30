use scylla::_macro_internal::{
    ColumnIterator, ColumnSpec, DeserializationError, DeserializeRow, TypeCheckError,
};
use scylla::value::CqlValue;
use scylla_cql::deserialize::value::DeserializeValue;

// A RustCqlRow represents a single row retrieved from a CQL query,
// with each column stored as a tuple of (column_name, CqlValue).
pub struct RustCqlRow {
    pub columns: Vec<(String, Option<CqlValue>)>,
}

impl DeserializeRow<'_, '_> for RustCqlRow {
    fn type_check(_specs: &[ColumnSpec]) -> Result<(), TypeCheckError> {
        Ok(())
    }

    fn deserialize(row: ColumnIterator) -> Result<Self, DeserializationError> {
        let mut cols = Vec::new();

        for col in row {
            let raw_col = col?;
            let value: Option<CqlValue> =
                Option::<CqlValue>::deserialize(raw_col.spec.typ(), raw_col.slice)?;
            cols.push((raw_col.spec.name().to_string(), value));
        }

        Ok(RustCqlRow { columns: cols })
    }
}
