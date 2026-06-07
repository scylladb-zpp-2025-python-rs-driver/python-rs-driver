from typing import Optional, Type

import pytest
from scylla.enums import Consistency
from scylla.execution_profile import ExecutionProfile
from scylla.retry_policy import (
    CqlResponseKind,
    DbError,
    DefaultRetryPolicy,
    DowngradingConsistencyRetryPolicy,
    FallthroughRetryPolicy,
    OperationType,
    RequestAttemptError,
    RequestInfo,
    RetryDecision,
    RetryPolicy,
    RetrySession,
    WriteType,
)


def test_default_retry_policy_isinstance():
    policy = DefaultRetryPolicy()
    assert isinstance(policy, DefaultRetryPolicy)
    assert isinstance(policy, RetryPolicy)


def test_downgrading_consistency_retry_policy_isinstance():
    policy = DowngradingConsistencyRetryPolicy()
    assert isinstance(policy, DowngradingConsistencyRetryPolicy)
    assert isinstance(policy, RetryPolicy)


def test_fallthrough_retry_policy_isinstance():
    policy = FallthroughRetryPolicy()
    assert isinstance(policy, FallthroughRetryPolicy)
    assert isinstance(policy, RetryPolicy)


def test_custom_retry_policy_isinstance():
    class MySession:
        def __init__(self):
            self.attempts = 0

        def decide_should_retry(self, request_info: RequestInfo) -> RetryDecision:
            self.attempts += 1
            if request_info.is_idempotent and self.attempts <= 2:
                return RetryDecision.DontRetry()
            return RetryDecision.DontRetry()

        def reset(self):
            self.attempts = 0

    class MyPolicy:
        def new_session(self):
            return MySession()

    policy = MyPolicy()
    session = policy.new_session()

    assert isinstance(policy, RetryPolicy)
    assert isinstance(session, RetrySession)


def test_default_retry_policy_identity_preserved():
    policy = DefaultRetryPolicy()
    profile = ExecutionProfile(retry_policy=policy)
    assert profile.retry_policy is policy


def test_downgrading_consistency_retry_policy_identity_preserved():
    policy = DowngradingConsistencyRetryPolicy()
    profile = ExecutionProfile(retry_policy=policy)
    assert profile.retry_policy is policy


def test_fallthrough_retry_policy_identity_preserved():
    policy = FallthroughRetryPolicy()
    profile = ExecutionProfile(retry_policy=policy)
    assert profile.retry_policy is policy


def test_no_retry_policy_is_none():
    profile = ExecutionProfile()
    assert profile.retry_policy is None


def test_explicit_none_retry_policy():
    profile = ExecutionProfile(retry_policy=None)
    assert profile.retry_policy is None


def test_custom_retry_policy_accepted():
    class MySession:
        def __init__(self):
            self.attempts = 0

        def decide_should_retry(self, request_info: RequestInfo) -> RetryDecision:
            self.attempts += 1
            if request_info.is_idempotent and self.attempts <= 2:
                return RetryDecision.DontRetry()
            return RetryDecision.DontRetry()

        def reset(self):
            self.attempts = 0

    class MyPolicy:
        def new_session(self) -> MySession:
            return MySession()

    policy = MyPolicy()
    profile = ExecutionProfile(retry_policy=policy)
    assert profile.retry_policy is policy


def test_different_profiles_have_independent_policies():
    policy_a = DefaultRetryPolicy()
    policy_b = FallthroughRetryPolicy()

    profile_a = ExecutionProfile(retry_policy=policy_a)
    profile_b = ExecutionProfile(retry_policy=policy_b)

    assert profile_a.retry_policy is policy_a
    assert profile_b.retry_policy is policy_b
    assert profile_a.retry_policy is not profile_b.retry_policy


def test_fallthrough_retry_policy_decide_should_retry():
    policy = FallthroughRetryPolicy()
    session = policy.new_session()
    request_info = RequestInfo(
        error=RequestAttemptError.SerializationError(), is_idempotent=False, consistency=Consistency.One
    )

    decision = session.decide_should_retry(request_info=request_info)

    assert isinstance(decision, RetryDecision.DontRetry)


def test_default_retry_policy_decide_should_retry():
    policy = DefaultRetryPolicy()
    session = policy.new_session()
    request_info = RequestInfo(
        error=RequestAttemptError.BrokenConnectionError(), is_idempotent=True, consistency=Consistency.One
    )

    decision = session.decide_should_retry(request_info=request_info)

    assert isinstance(decision, RetryDecision.RetryNextTarget)
    assert decision.consistency is None


@pytest.mark.parametrize(
    "error, is_idempotent, consistency, expected_decision_class",
    [
        (RequestAttemptError.SerializationError(), True, Consistency.Serial, RetryDecision.DontRetry),
        (RequestAttemptError.SerializationError(), True, Consistency.LocalSerial, RetryDecision.DontRetry),
        (RequestAttemptError.BrokenConnectionError(), True, Consistency.One, RetryDecision.RetryNextTarget),
        (RequestAttemptError.BrokenConnectionError(), False, Consistency.One, RetryDecision.DontRetry),
        (RequestAttemptError.UnableToAllocStreamId(), True, Consistency.One, RetryDecision.RetryNextTarget),
        (
            RequestAttemptError.UnableToAllocStreamId(),
            False,
            Consistency.One,
            RetryDecision.RetryNextTarget,
        ),
        (RequestAttemptError.BodyExtensionsParseError(), True, Consistency.One, RetryDecision.DontRetry),
        (RequestAttemptError.CqlErrorParseError(), True, Consistency.One, RetryDecision.DontRetry),
        (RequestAttemptError.CqlRequestSerialization(), True, Consistency.One, RetryDecision.DontRetry),
        (RequestAttemptError.CqlResultParseError(), True, Consistency.One, RetryDecision.DontRetry),
        (RequestAttemptError.NonfinishedPagingState(), True, Consistency.One, RetryDecision.DontRetry),
        (RequestAttemptError.RepreparedIdMissingInBatch(), True, Consistency.One, RetryDecision.DontRetry),
        (RequestAttemptError.SerializationError(), True, Consistency.One, RetryDecision.DontRetry),
        (RequestAttemptError.UnexpectedResponse(CqlResponseKind.Ready), True, Consistency.One, RetryDecision.DontRetry),
        (RequestAttemptError.RepreparedIdChanged("stmt", b"1", b"2"), True, Consistency.One, RetryDecision.DontRetry),
        (
            RequestAttemptError.DbError(DbError.Overloaded(), "msg"),
            True,
            Consistency.One,
            RetryDecision.RetryNextTarget,
        ),
        (RequestAttemptError.DbError(DbError.Overloaded(), "msg"), False, Consistency.One, RetryDecision.DontRetry),
        (
            RequestAttemptError.DbError(DbError.ServerError(), "msg"),
            True,
            Consistency.One,
            RetryDecision.RetryNextTarget,
        ),
        (RequestAttemptError.DbError(DbError.ServerError(), "msg"), False, Consistency.One, RetryDecision.DontRetry),
        (
            RequestAttemptError.DbError(DbError.TruncateError(), "msg"),
            True,
            Consistency.One,
            RetryDecision.RetryNextTarget,
        ),
        (RequestAttemptError.DbError(DbError.TruncateError(), "msg"), False, Consistency.One, RetryDecision.DontRetry),
        (
            RequestAttemptError.DbError(DbError.IsBootstrapping(), "msg"),
            False,
            Consistency.One,
            RetryDecision.RetryNextTarget,
        ),
        (RequestAttemptError.DbError(DbError.SyntaxError(), "msg"), True, Consistency.One, RetryDecision.DontRetry),
        (RequestAttemptError.DbError(DbError.Invalid(), "msg"), True, Consistency.One, RetryDecision.DontRetry),
        (
            RequestAttemptError.DbError(DbError.AuthenticationError(), "msg"),
            True,
            Consistency.One,
            RetryDecision.DontRetry,
        ),
        (RequestAttemptError.DbError(DbError.Unauthorized(), "msg"), True, Consistency.One, RetryDecision.DontRetry),
        (RequestAttemptError.DbError(DbError.ConfigError(), "msg"), True, Consistency.One, RetryDecision.DontRetry),
        (RequestAttemptError.DbError(DbError.ProtocolError(), "msg"), True, Consistency.One, RetryDecision.DontRetry),
        (
            RequestAttemptError.DbError(DbError.AlreadyExists("ks", "tbl"), "msg"),
            True,
            Consistency.One,
            RetryDecision.DontRetry,
        ),
        (
            RequestAttemptError.DbError(DbError.FunctionFailure("ks", "f", ["int"]), "msg"),
            True,
            Consistency.One,
            RetryDecision.DontRetry,
        ),
        (
            RequestAttemptError.DbError(DbError.ReadFailure(Consistency.One, 1, 2, 1, False), "msg"),
            True,
            Consistency.One,
            RetryDecision.DontRetry,
        ),
        (
            RequestAttemptError.DbError(DbError.WriteFailure(Consistency.One, 1, 2, 1, WriteType.Simple()), "msg"),
            True,
            Consistency.One,
            RetryDecision.DontRetry,
        ),
        (RequestAttemptError.DbError(DbError.Unprepared(b"id"), "msg"), True, Consistency.One, RetryDecision.DontRetry),
        (
            RequestAttemptError.DbError(DbError.RateLimitReached(OperationType.Read(), False), "msg"),
            True,
            Consistency.One,
            RetryDecision.DontRetry,
        ),
        (RequestAttemptError.DbError(DbError.Other(500), "msg"), True, Consistency.One, RetryDecision.DontRetry),
    ],
)
def test_retry_policy(
    error: RequestAttemptError,
    is_idempotent: bool,
    consistency: Consistency,
    expected_decision_class: Type[RetryDecision],
):
    policy = DefaultRetryPolicy()
    session = policy.new_session()

    request_info = RequestInfo(error=error, is_idempotent=is_idempotent, consistency=consistency)
    decision = session.decide_should_retry(request_info=request_info)

    assert isinstance(decision, expected_decision_class)


def test_db_error_unavailable_retry_at_most_once():
    policy = DefaultRetryPolicy()
    session = policy.new_session()

    request_info = RequestInfo(
        error=RequestAttemptError.DbError(DbError.Unavailable(Consistency.One, 2, 1), "msg"),
        is_idempotent=False,
        consistency=Consistency.One,
    )

    # First attempt: session.was_unavailable_retry is False -> should retry Next Target
    decision_1 = session.decide_should_retry(request_info=request_info)
    assert isinstance(decision_1, RetryDecision.RetryNextTarget)

    # Second attempt: session.was_unavailable_retry is now True -> should yield DontRetry
    decision_2 = session.decide_should_retry(request_info=request_info)
    assert isinstance(decision_2, RetryDecision.DontRetry)


def test_db_error_read_timeout_retry_conditions():
    policy = DefaultRetryPolicy()
    session = policy.new_session()

    # Case 1: Valid for retry (received >= required and data_present is False)
    valid_error = RequestAttemptError.DbError(
        DbError.ReadTimeout(consistency=Consistency.One, received=2, required=2, data_present=False), "msg"
    )
    request_info_valid = RequestInfo(error=valid_error, is_idempotent=False, consistency=Consistency.One)

    # First valid retry attempt -> should return RetrySameTarget
    decision_1 = session.decide_should_retry(request_info=request_info_valid)
    assert isinstance(decision_1, RetryDecision.RetrySameTarget)

    # Second valid retry attempt, was_read_timeout_retry is True -> should return DontRetry
    decision_2 = session.decide_should_retry(request_info=request_info_valid)
    assert isinstance(decision_2, RetryDecision.DontRetry)

    # Case 2: Invalid for retry (received < required)
    session_invalid_counts = policy.new_session()
    invalid_counts_error = RequestAttemptError.DbError(
        DbError.ReadTimeout(consistency=Consistency.One, received=1, required=2, data_present=False), "msg"
    )
    request_info_invalid_counts = RequestInfo(
        error=invalid_counts_error, is_idempotent=False, consistency=Consistency.One
    )
    assert isinstance(session_invalid_counts.decide_should_retry(request_info_invalid_counts), RetryDecision.DontRetry)

    # Case 3: Invalid for retry (data_present is True)
    session_invalid_data = policy.new_session()
    invalid_data_error = RequestAttemptError.DbError(
        DbError.ReadTimeout(consistency=Consistency.One, received=2, required=2, data_present=True), "msg"
    )
    request_info_invalid_data = RequestInfo(error=invalid_data_error, is_idempotent=False, consistency=Consistency.One)
    assert isinstance(session_invalid_data.decide_should_retry(request_info_invalid_data), RetryDecision.DontRetry)


def test_db_error_write_timeout_retry_conditions():
    policy = DefaultRetryPolicy()

    # Case 1: Valid for retry (is_idempotent=True and write_type == BatchLog)
    session_valid = policy.new_session()
    valid_error = RequestAttemptError.DbError(
        DbError.WriteTimeout(consistency=Consistency.One, received=1, required=2, write_type=WriteType.BatchLog()),
        "msg",
    )
    request_info_valid = RequestInfo(error=valid_error, is_idempotent=True, consistency=Consistency.One)

    # First attempt -> RetrySameTarget
    assert isinstance(session_valid.decide_should_retry(request_info=request_info_valid), RetryDecision.RetrySameTarget)
    # Second attempt -> DontRetry
    assert isinstance(session_valid.decide_should_retry(request_info=request_info_valid), RetryDecision.DontRetry)

    # Case 2: Non-idempotent request should immediately fail
    session_non_idempotent = policy.new_session()
    request_info_non_idempotent = RequestInfo(error=valid_error, is_idempotent=False, consistency=Consistency.One)
    assert isinstance(session_non_idempotent.decide_should_retry(request_info_non_idempotent), RetryDecision.DontRetry)

    # Case 3: WriteType is Simple instead of BatchLog should fail
    session_wrong_type = policy.new_session()
    invalid_type_error = RequestAttemptError.DbError(
        DbError.WriteTimeout(consistency=Consistency.One, received=1, required=2, write_type=WriteType.Simple()), "msg"
    )
    request_info_wrong_type = RequestInfo(error=invalid_type_error, is_idempotent=True, consistency=Consistency.One)
    assert isinstance(session_wrong_type.decide_should_retry(request_info_wrong_type), RetryDecision.DontRetry)


@pytest.mark.parametrize(
    "error, is_idempotent, consistency, expected_class, expected_cl",
    [
        (
            RequestAttemptError.DbError(DbError.Unavailable(Consistency.Serial, 1, 1), "msg"),
            True,
            Consistency.Serial,
            RetryDecision.RetryNextTarget,
            None,
        ),
        (
            RequestAttemptError.DbError(DbError.Unavailable(Consistency.LocalSerial, 1, 1), "msg"),
            True,
            Consistency.LocalSerial,
            RetryDecision.RetryNextTarget,
            None,
        ),
        (RequestAttemptError.SerializationError(), True, Consistency.Serial, RetryDecision.DontRetry, None),
        (RequestAttemptError.BrokenConnectionError(), True, Consistency.One, RetryDecision.RetryNextTarget, None),
        (RequestAttemptError.BrokenConnectionError(), False, Consistency.One, RetryDecision.DontRetry, None),
        (RequestAttemptError.UnableToAllocStreamId(), True, Consistency.One, RetryDecision.RetryNextTarget, None),
        (RequestAttemptError.UnableToAllocStreamId(), False, Consistency.One, RetryDecision.RetryNextTarget, None),
        (RequestAttemptError.BodyExtensionsParseError(), True, Consistency.One, RetryDecision.DontRetry, None),
        (RequestAttemptError.CqlErrorParseError(), True, Consistency.One, RetryDecision.DontRetry, None),
        (RequestAttemptError.CqlRequestSerialization(), True, Consistency.One, RetryDecision.DontRetry, None),
        (RequestAttemptError.CqlResultParseError(), True, Consistency.One, RetryDecision.DontRetry, None),
        (RequestAttemptError.NonfinishedPagingState(), True, Consistency.One, RetryDecision.DontRetry, None),
        (RequestAttemptError.RepreparedIdMissingInBatch(), True, Consistency.One, RetryDecision.DontRetry, None),
        (RequestAttemptError.SerializationError(), True, Consistency.One, RetryDecision.DontRetry, None),
        (
            RequestAttemptError.UnexpectedResponse(CqlResponseKind.Ready),
            True,
            Consistency.One,
            RetryDecision.DontRetry,
            None,
        ),
        (
            RequestAttemptError.RepreparedIdChanged("stmt", b"1", b"2"),
            True,
            Consistency.One,
            RetryDecision.DontRetry,
            None,
        ),
        (
            RequestAttemptError.DbError(DbError.Overloaded(), "msg"),
            True,
            Consistency.One,
            RetryDecision.RetryNextTarget,
            None,
        ),
        (
            RequestAttemptError.DbError(DbError.Overloaded(), "msg"),
            False,
            Consistency.One,
            RetryDecision.DontRetry,
            None,
        ),
        (
            RequestAttemptError.DbError(DbError.ServerError(), "msg"),
            True,
            Consistency.One,
            RetryDecision.RetryNextTarget,
            None,
        ),
        (
            RequestAttemptError.DbError(DbError.ServerError(), "msg"),
            False,
            Consistency.One,
            RetryDecision.DontRetry,
            None,
        ),
        (
            RequestAttemptError.DbError(DbError.TruncateError(), "msg"),
            True,
            Consistency.One,
            RetryDecision.RetryNextTarget,
            None,
        ),
        (
            RequestAttemptError.DbError(DbError.TruncateError(), "msg"),
            False,
            Consistency.One,
            RetryDecision.DontRetry,
            None,
        ),
        (
            RequestAttemptError.DbError(DbError.IsBootstrapping(), "msg"),
            False,
            Consistency.One,
            RetryDecision.RetryNextTarget,
            None,
        ),
        (
            RequestAttemptError.DbError(DbError.SyntaxError(), "msg"),
            True,
            Consistency.One,
            RetryDecision.DontRetry,
            None,
        ),
        (RequestAttemptError.DbError(DbError.Invalid(), "msg"), True, Consistency.One, RetryDecision.DontRetry, None),
        (
            RequestAttemptError.DbError(DbError.AuthenticationError(), "msg"),
            True,
            Consistency.One,
            RetryDecision.DontRetry,
            None,
        ),
        (
            RequestAttemptError.DbError(DbError.Unauthorized(), "msg"),
            True,
            Consistency.One,
            RetryDecision.DontRetry,
            None,
        ),
        (
            RequestAttemptError.DbError(DbError.ConfigError(), "msg"),
            True,
            Consistency.One,
            RetryDecision.DontRetry,
            None,
        ),
        (
            RequestAttemptError.DbError(DbError.ProtocolError(), "msg"),
            True,
            Consistency.One,
            RetryDecision.DontRetry,
            None,
        ),
        (
            RequestAttemptError.DbError(DbError.AlreadyExists("ks", "tbl"), "msg"),
            True,
            Consistency.One,
            RetryDecision.DontRetry,
            None,
        ),
        (
            RequestAttemptError.DbError(DbError.FunctionFailure("ks", "f", ["int"]), "msg"),
            True,
            Consistency.One,
            RetryDecision.DontRetry,
            None,
        ),
        (
            RequestAttemptError.DbError(DbError.ReadFailure(Consistency.One, 1, 2, 1, False), "msg"),
            True,
            Consistency.One,
            RetryDecision.DontRetry,
            None,
        ),
        (
            RequestAttemptError.DbError(DbError.WriteFailure(Consistency.One, 1, 2, 1, WriteType.Simple()), "msg"),
            True,
            Consistency.One,
            RetryDecision.DontRetry,
            None,
        ),
        (
            RequestAttemptError.DbError(DbError.Unprepared(b"id"), "msg"),
            True,
            Consistency.One,
            RetryDecision.DontRetry,
            None,
        ),
        (
            RequestAttemptError.DbError(DbError.RateLimitReached(OperationType.Read(), False), "msg"),
            True,
            Consistency.One,
            RetryDecision.DontRetry,
            None,
        ),
        (RequestAttemptError.DbError(DbError.Other(500), "msg"), True, Consistency.One, RetryDecision.DontRetry, None),
        (
            RequestAttemptError.DbError(DbError.WriteTimeout(Consistency.Quorum, 1, 3, WriteType.Counter()), "msg"),
            True,
            Consistency.Quorum,
            RetryDecision.DontRetry,
            None,
        ),
        (
            RequestAttemptError.DbError(DbError.WriteTimeout(Consistency.Quorum, 1, 3, WriteType.Cas()), "msg"),
            True,
            Consistency.Quorum,
            RetryDecision.DontRetry,
            None,
        ),
        (
            RequestAttemptError.DbError(DbError.WriteTimeout(Consistency.Quorum, 1, 3, WriteType.View()), "msg"),
            True,
            Consistency.Quorum,
            RetryDecision.DontRetry,
            None,
        ),
        (
            RequestAttemptError.DbError(DbError.WriteTimeout(Consistency.Quorum, 1, 3, WriteType.Cdc()), "msg"),
            True,
            Consistency.Quorum,
            RetryDecision.DontRetry,
            None,
        ),
        (
            RequestAttemptError.DbError(
                DbError.WriteTimeout(Consistency.Quorum, 1, 3, WriteType.Other("custom")), "msg"
            ),
            True,
            Consistency.Quorum,
            RetryDecision.DontRetry,
            None,
        ),
        (
            RequestAttemptError.DbError(DbError.WriteTimeout(Consistency.Quorum, 0, 3, WriteType.Simple()), "msg"),
            True,
            Consistency.Quorum,
            RetryDecision.DontRetry,
            None,
        ),
    ],
)
def test_downgrading_policy(
    error: RequestAttemptError,
    is_idempotent: bool,
    consistency: Consistency,
    expected_class: Type[RetryDecision],
    expected_cl: Optional[Consistency],
) -> None:
    policy = DowngradingConsistencyRetryPolicy()
    session = policy.new_session()

    request_info = RequestInfo(error=error, is_idempotent=is_idempotent, consistency=consistency)
    decision = session.decide_should_retry(request_info=request_info)

    assert isinstance(decision, expected_class)
    if expected_cl is not None and hasattr(decision, "consistency"):
        assert getattr(decision, "consistency") == expected_cl


@pytest.mark.parametrize(
    "alive, current_cl, expected_downgraded_cl",
    [
        (3, Consistency.Quorum, Consistency.Three),
        (2, Consistency.Quorum, Consistency.Two),
        (1, Consistency.Quorum, Consistency.One),
        (0, Consistency.EachQuorum, Consistency.One),
    ],
)
def test_unavailable_downgrade(alive: int, current_cl: Consistency, expected_downgraded_cl: Consistency) -> None:
    policy = DowngradingConsistencyRetryPolicy()
    session = policy.new_session()

    error = RequestAttemptError.DbError(DbError.Unavailable(current_cl, 3, alive), "msg")
    request_info = RequestInfo(error=error, is_idempotent=True, consistency=current_cl)

    # First attempt
    decision_1 = session.decide_should_retry(request_info=request_info)
    assert isinstance(decision_1, RetryDecision.RetrySameTarget)
    assert decision_1.consistency == expected_downgraded_cl

    # Second sequential attempt must return DontRetry (was_retry = true)
    decision_2 = session.decide_should_retry(request_info=request_info)
    assert isinstance(decision_2, RetryDecision.DontRetry)


@pytest.mark.parametrize(
    "received, required, data_present, expected_class, expected_cl",
    [
        (2, 3, True, RetryDecision.RetrySameTarget, Consistency.Two),
        (3, 3, False, RetryDecision.RetrySameTarget, None),
        (3, 3, True, RetryDecision.DontRetry, None),
    ],
)
def test_read_timeout(
    received: int,
    required: int,
    data_present: bool,
    expected_class: Type[RetryDecision],
    expected_cl: Optional[Consistency],
) -> None:
    policy = DowngradingConsistencyRetryPolicy()
    session = policy.new_session()

    error = RequestAttemptError.DbError(
        DbError.ReadTimeout(Consistency.Quorum, received, required, data_present), "msg"
    )
    request_info = RequestInfo(error=error, is_idempotent=True, consistency=Consistency.Quorum)

    decision = session.decide_should_retry(request_info=request_info)
    assert isinstance(decision, expected_class)
    if expected_cl is not None and hasattr(decision, "consistency"):
        assert getattr(decision, "consistency") == expected_cl


@pytest.mark.parametrize(
    "write_type, received, expected_class, expected_cl",
    [
        (WriteType.Simple(), 1, RetryDecision.IgnoreWriteError, None),
        (WriteType.Batch(), 1, RetryDecision.IgnoreWriteError, None),
        (WriteType.UnloggedBatch(), 2, RetryDecision.RetrySameTarget, Consistency.Two),
        (WriteType.BatchLog(), 0, RetryDecision.RetrySameTarget, None),
    ],
)
def test_write_timeout(
    write_type: WriteType, received: int, expected_class: Type[RetryDecision], expected_cl: Optional[Consistency]
) -> None:
    policy = DowngradingConsistencyRetryPolicy()
    session = policy.new_session()

    error = RequestAttemptError.DbError(DbError.WriteTimeout(Consistency.Quorum, received, 3, write_type), "msg")
    request_info = RequestInfo(error=error, is_idempotent=True, consistency=Consistency.Quorum)

    decision = session.decide_should_retry(request_info=request_info)
    assert isinstance(decision, expected_class)
    if expected_cl is not None and hasattr(decision, "consistency"):
        assert getattr(decision, "consistency") == expected_cl
