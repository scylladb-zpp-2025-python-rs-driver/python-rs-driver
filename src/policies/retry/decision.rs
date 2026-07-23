use crate::enums::PyConsistency;
use pyo3::prelude::*;
use scylla::policies::retry::RetryDecision;

#[pyclass(name = "RetryDecision", frozen, from_py_object)]
#[derive(Debug, Clone)]
pub(crate) enum PyRetryDecision {
    #[pyo3(constructor = (consistency = None))]
    RetrySameTarget {
        consistency: Option<PyConsistency>,
    },
    #[pyo3(constructor = (consistency = None))]
    RetryNextTarget {
        consistency: Option<PyConsistency>,
    },
    DontRetry(),
    IgnoreWriteError(),
}

impl From<&PyRetryDecision> for RetryDecision {
    fn from(value: &PyRetryDecision) -> Self {
        match value {
            PyRetryDecision::RetrySameTarget { consistency } => {
                RetryDecision::RetrySameTarget(consistency.map(Into::into))
            }
            PyRetryDecision::RetryNextTarget { consistency } => {
                RetryDecision::RetryNextTarget(consistency.map(Into::into))
            }
            PyRetryDecision::DontRetry() => RetryDecision::DontRetry,
            PyRetryDecision::IgnoreWriteError() => RetryDecision::IgnoreWriteError,
        }
    }
}

impl From<RetryDecision> for PyRetryDecision {
    #[deny(clippy::wildcard_enum_match_arm)]
    fn from(value: RetryDecision) -> Self {
        match value {
            RetryDecision::RetrySameTarget(consistency) => PyRetryDecision::RetrySameTarget {
                consistency: consistency.map(Into::into),
            },
            RetryDecision::RetryNextTarget(consistency) => PyRetryDecision::RetryNextTarget {
                consistency: consistency.map(Into::into),
            },
            RetryDecision::DontRetry => PyRetryDecision::DontRetry(),
            RetryDecision::IgnoreWriteError => PyRetryDecision::IgnoreWriteError(),
            _ => unreachable!("Unhandled `RetryDecision` variant"),
        }
    }
}
