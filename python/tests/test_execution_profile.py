import pytest
from scylla.enums import Consistency, SerialConsistency
from scylla.execution_profile import ExecutionProfile


def test_execution_profile_builder():
    profile = ExecutionProfile()
    assert isinstance(profile, ExecutionProfile)


def test_execution_profile_negative_timeout():
    with pytest.raises(ValueError) as exc_info:
        ExecutionProfile(timeout=-1.0)
    assert "timeout must be a positive, finite number" in str(exc_info.value)


def test_execution_profile_zero_timeout():
    with pytest.raises(ValueError) as exc_info:
        ExecutionProfile(timeout=0.0)
    assert "timeout must be a positive, finite number" in str(exc_info.value)


def test_execution_profile_nan_timeout():
    with pytest.raises(ValueError) as exc_info:
        ExecutionProfile(timeout=float("nan"))
    assert "timeout must be a positive, finite number" in str(exc_info.value)


def test_execution_profile_infinity_timeout():
    with pytest.raises(ValueError) as exc_info:
        ExecutionProfile(timeout=float("inf"))
    assert "timeout must be a positive, finite number" in str(exc_info.value)


def test_execution_profile_builder_consistency():
    expected_consistency = Consistency.One
    profile = ExecutionProfile(consistency=expected_consistency)
    assert isinstance(profile, ExecutionProfile)
    actual_consistency = profile.get_consistency()
    assert actual_consistency != Consistency.Two
    assert actual_consistency == expected_consistency


def test_execution_profile_builder_serial_consistency():
    expected_serial_consistency = SerialConsistency.Serial
    profile = ExecutionProfile(serial_consistency=expected_serial_consistency)
    assert isinstance(profile, ExecutionProfile)
    actual_serial_consistency = profile.get_serial_consistency()
    assert actual_serial_consistency != SerialConsistency.LocalSerial
    assert actual_serial_consistency == expected_serial_consistency


def test_execution_profile_timeout():
    expected_timeout = 10.5
    profile = ExecutionProfile(timeout=expected_timeout)
    assert isinstance(profile, ExecutionProfile)
    actual_timeout = profile.get_request_timeout()
    assert actual_timeout == expected_timeout
