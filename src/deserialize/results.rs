use crate::deserialize::PyDeserializationError;
use crate::deserialize::value::{PyDeserializeValue, PyDeserializedValue};
use crate::session::Session;
use pyo3::exceptions::PyStopIteration;
use pyo3::prelude::{PyDictMethods, PyModule, PyModuleMethods};
use pyo3::types::{PyDict, PyString};
use pyo3::{Bound, Py, PyAny, PyErr, PyRefMut, PyResult, Python, pyclass, pymethods, pymodule};
use scylla::response::query_result::QueryResult;
use scylla_cql::deserialize::FrameSlice;
use scylla_cql::deserialize::result::RawRowIterator;
use scylla_cql::deserialize::row::ColumnIterator;
use scylla_cql::frame::request::query::{PagingState, PagingStateResponse};
use stable_deref_trait::StableDeref;
use std::ops::Deref;
use std::sync::Arc;
use yoke::{Yoke, Yokeable};

/// Result of a single request to the database. It represents any kind of Result frame.
#[pyclass(frozen)]
pub(crate) struct RequestResult {
    pub(crate) inner: Arc<QueryResult>,
}

#[pymethods]
impl RequestResult {
    /// Iterate over rows returned by the query.
    ///
    /// This method returns a Python iterator yielding rows from the result set.
    /// Each row is automatically deserialized and materialized using a
    /// `RowFactory`.
    ///
    /// By default, rows are returned as Python dictionaries mapping column
    /// names (`str`) to deserialized values. A custom `RowFactory` may be
    /// provided to control how rows are constructed.
    ///
    /// Parameters
    /// ----------
    /// factory : RowFactory, optional
    ///     Custom factory used to build each row.
    ///
    /// Returns
    /// -------
    /// RowsIterator
    ///     An iterator yielding deserialized rows.
    #[pyo3(signature = (factory=None))]
    fn iter_rows<'py>(
        &self,
        py: Python<'py>,
        factory: Option<Bound<RowFactory>>,
    ) -> PyResult<RowsIterator> {
        RowsIterator::new(py, &self.inner, factory)
    }
}

//TODO
//Ask Do We want to have separate results for paging queries and normal queries
#[pyclass]
pub(crate) struct PagingRequestResult {
    paging_response: PagingStateResponse,
    session: Session,
    prepared_statement: scylla::statement::prepared::PreparedStatement,
    query_result: Arc<QueryResult>,
}

impl PagingRequestResult {
    pub(crate) fn new(
        paging_response: PagingStateResponse,
        session: Session,
        prepared_statement: scylla::statement::prepared::PreparedStatement,
        query_result: QueryResult,
    ) -> Self {
        PagingRequestResult {
            paging_response,
            session,
            prepared_statement,
            query_result: Arc::new(query_result),
        }
    }
}

#[pymethods]
impl PagingRequestResult {
    fn has_more_pages(&self) -> bool {
        matches!(
            &self.paging_response,
            PagingStateResponse::HasMorePages { .. }
        )
    }

    fn paging_state(&self) -> Option<PyPagingState> {
        match &self.paging_response {
            PagingStateResponse::HasMorePages { state } => Some(PyPagingState {
                inner: state.clone(),
            }),
            _ => None,
        }
    }

    // TODO
    // Investigate if when we are mutating variables in python We need to put them in MUTEX!!!
    async fn fetch_next_page(&mut self) -> PyResult<()> {
        let state = match &self.paging_response {
            PagingStateResponse::HasMorePages { state } => state.clone(),
            _ => return Ok(()),
        };

        let (query_result, paging_response) = self
            .session
            .execute_single_page(state, self.prepared_statement.clone())
            .await?;

        self.paging_response = paging_response;
        self.query_result = Arc::new(query_result);

        Ok(())
    }

    #[pyo3(signature = (factory=None))]
    fn iter_page<'py>(
        &self,
        py: Python<'py>,
        factory: Option<Bound<RowFactory>>,
    ) -> PyResult<RowsIterator> {
        RowsIterator::new(py, &self.query_result, factory)
    }
}

enum RowsIteratorKind {
    Rows {
        row_col_cursor: Py<RowColumnCursor>,
        factory: Py<RowFactory>,
    },
    NonRows,
}

impl RowsIteratorKind {
    fn next(&self, py: Python) -> PyResult<Py<PyAny>> {
        match self {
            RowsIteratorKind::Rows {
                row_col_cursor,
                factory,
            } => {
                row_col_cursor
                    .borrow_mut(py)
                    .yoked
                    .with_mut_return(|cursor| cursor.next_row())?;

                factory.call_method1(py, "build", (&row_col_cursor.bind(py),))
            }
            RowsIteratorKind::NonRows => Err(PyErr::new::<PyStopIteration, _>("")),
        }
    }
}

/// Iterator yielding deserialized rows from a query result.
///
/// `RowsIterator` behaves like a standard Python iterator. Each iteration
/// returns a single row produced by the query and materialized using a
/// `RowFactory`.
///
/// By default, rows are returned as Python dictionaries mapping column
/// names to values.
#[pyclass]
struct RowsIterator {
    kind: RowsIteratorKind,
}

impl RowsIterator {
    fn new<'py>(
        py: Python<'py>,
        query_result: &Arc<QueryResult>,
        factory: Option<Bound<RowFactory>>,
    ) -> PyResult<Self> {
        if !query_result.is_rows() {
            return Ok(RowsIterator {
                kind: RowsIteratorKind::NonRows,
            });
        }

        let factory = match factory {
            Some(b) => b.unbind(),
            None => Py::new(py, RowFactory::new())?,
        };

        let row_col_cursor = Py::new(py, RowColumnCursor::new(Arc::clone(query_result)))?;

        Ok(RowsIterator {
            kind: RowsIteratorKind::Rows {
                row_col_cursor,
                factory,
            },
        })
    }
}

#[pymethods]
impl RowsIterator {
    pub fn __next__(slf: PyRefMut<'_, Self>) -> PyResult<Py<PyAny>> {
        let py = slf.py();
        slf.kind.next(py)
    }

    pub fn __iter__(slf: PyRefMut<'_, Self>) -> PyRefMut<'_, Self> {
        slf
    }
}

/// Yoke-backed wrapper holding row and column iterators.
///
/// `Cursor` is stored inside a `Yoke` so that both the row iterator
///  and the column iterator can borrow from the same data without cloning.
///
/// - `next_row` advances the row iterator and switches the active column
///   iterator to the value received from row iterator.
/// - `next_column` advances the column iterator and deserializes column values
///   into Python objects.
#[derive(Yokeable)]
struct Cursor<'a> {
    row_iterator: RawRowIterator<'a, 'a>,
    column_iterator: ColumnIterator<'a, 'a>,
}

impl<'a> Cursor<'a> {
    fn next_column(&mut self) -> PyResult<Column> {
        Python::attach(|py| {
            let raw_col = self
                .column_iterator
                .next()
                .ok_or_else(|| PyErr::new::<PyStopIteration, _>(""))?
                .map_err(PyDeserializationError::from)?;

            let value = PyDeserializedValue::deserialize_py(raw_col.spec.typ(), raw_col.slice, py)?;

            let column_name = PyString::new(py, raw_col.spec.name()).unbind();

            Ok(Column { column_name, value })
        })
    }

    fn next_row(&mut self) -> PyResult<()> {
        let column_iterator = self
            .row_iterator
            .next()
            .ok_or_else(|| PyErr::new::<PyStopIteration, _>(""))?
            .map_err(PyDeserializationError::from)?;

        self.column_iterator = column_iterator;
        Ok(())
    }
}

/// Stable cart holding deserialized metadata and raw row data.
///
/// This type exists solely to serve as a `StableDeref` cart for `Yoke`.
struct QueryResultCart(Arc<QueryResult>);

impl Deref for QueryResultCart {
    type Target = QueryResult;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

unsafe impl StableDeref for QueryResultCart {}

/// Iterator over columns of the current row.
///
/// This object is passed to `RowFactory.build` and allows iterating over
/// column values of a single row. Each iteration yields a `Column` object
/// containing the column name and its deserialized value.
///
/// This iterator is only intended to be consumed while building a row and
/// should not be stored or reused outside of that context.
#[pyclass(name = "ColumnIterator")]
pub struct RowColumnCursor {
    // Yoke-backed container holding both row and column iterators.
    //
    // The yoke ensures that iterators can borrow directly from the
    // underlying query result frame without cloning buffers or allocating
    // intermediate representations.
    //
    // `Cursor` holds:
    // - a `RawRowIterator` to advance between rows
    // - a `ColumnIterator` for iterating columns of the current row
    yoked: Yoke<Cursor<'static>, QueryResultCart>,
}

impl RowColumnCursor {
    fn new(query_result: Arc<QueryResult>) -> Self {
        let cart = QueryResultCart(query_result);

        let yoked = Yoke::attach_to_cart(cart, |cart| {
            let raw_rows_with_metadata = cart.deserialized_metadata_and_rows().expect(
                "deserialized_metadata_and_rows can't be None after is_rows() returned true",
            );
            let frame_slice = FrameSlice::new(raw_rows_with_metadata.raw_rows());
            let col_specs = raw_rows_with_metadata.metadata().col_specs();
            let row_iterator =
                RawRowIterator::new(raw_rows_with_metadata.rows_count(), col_specs, frame_slice);

            let column_iterator = ColumnIterator::new(col_specs, frame_slice);

            Cursor {
                row_iterator,
                column_iterator,
            }
        });

        Self { yoked }
    }
}

#[pymethods]
impl RowColumnCursor {
    pub fn __next__(&mut self) -> PyResult<Column> {
        self.yoked.with_mut_return(|view| view.next_column())
    }
    pub fn __iter__(slf: PyRefMut<'_, Self>) -> PyRefMut<'_, Self> {
        slf
    }
}

#[pyclass(name = "PagingState")]
pub struct PyPagingState {
    pub(crate) inner: PagingState,
}

#[pymethods]
impl PyPagingState {
    #[new]
    fn new() -> Self {
        PyPagingState {
            inner: PagingState::start(),
        }
    }
}

/// A single column value within a row.
///
/// `Column` represents one column of a row returned by a query. It contains
/// the column name and the corresponding deserialized Python value.
#[pyclass(frozen)]
pub struct Column {
    #[pyo3(get)]
    column_name: Py<PyString>,
    #[pyo3(get)]
    value: PyDeserializedValue,
}

/// Factory responsible for constructing Python row objects.
///
/// `RowFactory` defines how a row is materialized from a column iterator.
/// The default implementation consumes all columns of the current row and
/// returns a Python dictionary mapping column names to values.
///
/// Users may subclass this type to implement custom row mappings.
#[pyclass(subclass, frozen)]
pub struct RowFactory {}

#[pymethods]
impl RowFactory {
    /// Create a new `RowFactory`.
    ///
    /// The default row factory builds each row as a Python `dict`
    /// mapping column names to deserialized Python values.
    #[new]
    pub fn new() -> Self {
        RowFactory {}
    }

    /// Build a Python object representing a single row.
    ///
    /// This method consumes all columns from the provided column iterator
    /// and returns a Python `dict` mapping column names to values.
    ///
    /// Parameters
    /// ----------
    /// column_iterator : RowColumnCursor
    ///     Iterator over columns of the current row.
    ///
    /// Returns
    /// -------
    /// dict
    ///     A dictionary mapping column names (`str`) to deserialized
    ///     Python values.
    ///
    /// Raises
    /// ------
    /// RuntimeError
    ///     If deserialization of any column fails.
    pub fn build<'py>(
        &self,
        py: Python<'py>,
        column_iterator: &Bound<'py, RowColumnCursor>,
    ) -> PyResult<Py<PyDict>> {
        let mut columns = column_iterator.borrow_mut();

        let dict = PyDict::new(py);
        loop {
            match columns.__next__() {
                Ok(column) => dict.set_item(column.column_name, column.value)?,
                Err(err) if err.is_instance_of::<PyStopIteration>(py) => break,
                Err(err) => return Err(err),
            }
        }

        Ok(dict.into())
    }
}

impl Default for RowFactory {
    fn default() -> Self {
        RowFactory::new()
    }
}

#[pymodule]
pub(crate) fn results(_py: Python<'_>, module: &Bound<'_, PyModule>) -> PyResult<()> {
    module.add_class::<RowFactory>()?;
    module.add_class::<Column>()?;
    module.add_class::<RequestResult>()?;
    module.add_class::<RowColumnCursor>()?;
    module.add_class::<RowsIterator>()?;
    module.add_class::<PyPagingState>()?;
    module.add_class::<PagingRequestResult>()?;

    Ok(())
}
