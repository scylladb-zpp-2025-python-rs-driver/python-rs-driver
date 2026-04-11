import pytest

from scylla.session_builder import SessionBuilder
from scylla.errors import SessionConfigError, SessionConnectionError as DriverConnectionError


def test_session_builder_invalid_port_negative():
    with pytest.raises(SessionConfigError) as exc_info:
        SessionBuilder(["127.0.0.1"], -1)

    assert "expected an integer between 0 and 65535" in str(exc_info.value)
    assert exc_info.value.__cause__ is not None


def test_session_builder_invalid_port_too_large():
    with pytest.raises(SessionConfigError) as exc_info:
        SessionBuilder(["127.0.0.1"], 70000)

    assert "expected an integer between 0 and 65535" in str(exc_info.value)
    assert exc_info.value.__cause__ is not None


def test_session_builder_contact_points_cannot_be_string():
    with pytest.raises(SessionConfigError) as exc_info:
        SessionBuilder("127.0.0.1", 9042)

    assert "contact_points should be a sequence of strings, not a string" in str(exc_info.value)


def test_session_builder_contact_point_item_must_be_string():
    with pytest.raises(SessionConfigError) as exc_info:
        SessionBuilder(["127.0.0.1", 123], 9042)  # type: ignore[reportArgumentType]

    assert "expected a string" in str(exc_info.value)
    assert getattr(exc_info.value, "index", None) == 1


@pytest.mark.asyncio
async def test_session_builder_connect_raises_connection_error_for_unreachable_node():
    builder = SessionBuilder(["127.0.0.1"], 1)

    with pytest.raises(DriverConnectionError) as exc_info:
        await builder.connect()

    assert "failed to establish session" in str(exc_info.value)


def test_session_builder_contact_point_conversion_failed_sets_index():
    bad = "\ud800"

    with pytest.raises(SessionConfigError) as exc_info:
        SessionBuilder([bad], 9042)

    assert "Failed to convert contact point at index 0 to string" in str(exc_info.value)
    assert getattr(exc_info.value, "index", None) == 0
    assert exc_info.value.__cause__ is not None
