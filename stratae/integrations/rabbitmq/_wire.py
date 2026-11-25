"""How a message carries its envelope, its headers and its failures on the wire."""

from __future__ import annotations

import logging
from collections.abc import Callable, Mapping
from copy import copy
from datetime import datetime
from decimal import Decimal
from types import MappingProxyType, NoneType
from typing import TYPE_CHECKING
from uuid import UUID

from pamqp.commands import Basic
from pamqp.common import FieldTable

from stratae.events import Envelope, EnvelopeHeaders
from stratae.events.failure import failure_reason
from stratae.events.middleware import NO_HEADERS
from stratae.integrations.rabbitmq.config import ATTEMPTS_HEADER, FAILURE_HEADER

if TYPE_CHECKING:
    from aiormq.abc import DeliveredMessage


_log = logging.getLogger(__name__)

_HEADERS = EnvelopeHeaders()
"""The names the adapter writes an envelope under, and reads it from."""


def stamp(
    properties: Basic.Properties | None, envelope: Envelope, headers: Mapping[str, object]
) -> Basic.Properties:
    """Copy properties, stamped with the message's headers, envelope ids, and timestamp."""
    stamped = copy(properties) if properties is not None else Basic.Properties()
    merged = dict(stamped.headers or {})
    if headers:
        merged |= _field_table(headers)
    stamped.headers = merged | _HEADERS.write(envelope)
    stamped.message_id = str(envelope.message_id)
    stamped.correlation_id = str(envelope.correlation_id)
    stamped.timestamp = envelope.timestamp
    return stamped


_FIELD_VALUES = (bool, bytes, bytearray, Decimal, dict, list, float, int, str, datetime, NoneType)
"""The types an AMQP header value can be, as pamqp encodes them."""


def _field_table(headers: Mapping[str, object]) -> FieldTable:
    """
    Return headers as an AMQP field table, refusing a value AMQP can't carry.

    Values inside a list or a table are checked by pamqp, when the message is encoded.

    :raises TypeError: If a header's value isn't one AMQP carries.
    """
    table: FieldTable = {}
    for name, value in headers.items():
        if not isinstance(value, _FIELD_VALUES):
            raise TypeError(_UNCARRIED.format(name, type(value).__name__))
        table[name] = value
    return table


_UNCARRIED = "AMQP can't carry header {!r} of type {}"


def envelope_from(message: DeliveredMessage, new_id: Callable[[], UUID]) -> Envelope | None:
    """Rebuild the Envelope carried by a delivered message, making any missing id with `new_id`."""
    properties = message.header.properties
    native = {
        _HEADERS.message_id: properties.message_id,
        _HEADERS.correlation_id: properties.correlation_id,
        _HEADERS.timestamp: properties.timestamp and properties.timestamp.isoformat(),
    }
    headers = native | dict(properties.headers or {})
    message_id, correlation_id = _HEADERS.message_id, _HEADERS.correlation_id
    if (headers.get(message_id) is None) != (headers.get(correlation_id) is None):
        _log.info("partial envelope headers; minting the missing id: %r", headers)
    try:
        return _HEADERS.read(headers, new_id)
    except ValueError:
        _log.warning("unparseable envelope headers; scoping a fresh envelope: %r", headers)
        return None


def headers_from(message: DeliveredMessage) -> Mapping[str, object]:
    """
    Return a delivered message's headers without its envelope's, read-only, for handle middleware.

    The envelope is rebuilt from its headers by {py:func}`envelope_from`, so handle middleware
    reads it as a value, as it does on a direct bus.
    """
    delivered = message.header.properties.headers
    if not delivered:
        return NO_HEADERS
    headers = dict(delivered)
    for name in _HEADERS.names:
        headers.pop(name, None)
    return MappingProxyType(headers)


def failed_attempts(message: DeliveredMessage) -> int:
    """Return how many times a delivered message has already failed."""
    attempts = (message.header.properties.headers or {}).get(ATTEMPTS_HEADER)
    return attempts if isinstance(attempts, int) and attempts > 0 else 0


def failed_properties(message: DeliveredMessage, attempts: int, exc: Exception) -> Basic.Properties:
    """Copy a failed message's properties, stamped with its attempts and failure."""
    properties = copy(message.header.properties)
    properties.headers = dict(properties.headers or {}) | {
        ATTEMPTS_HEADER: attempts,
        FAILURE_HEADER: failure_reason(exc),
    }
    return properties
