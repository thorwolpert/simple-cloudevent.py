

import json
import time
from datetime import datetime, timezone

from freezegun import freeze_time

from simple_cloudevent import SIMPLE_CE_SPEC_VERSION, SIMPLE_CE_SPEC_CONTENTTYPE
from simple_cloudevent import SimpleCloudEvent
from simple_cloudevent import to_structured
from simple_cloudevent import to_queue_message


def test_to_queue_message():
    ce = SimpleCloudEvent()

    msg = to_queue_message(ce)

    assert msg
    assert isinstance(msg, bytes)

    ce_msg = json.loads(msg.decode('UTF8'))
    assert isinstance(ce_msg, dict)


def test_with_datetime_to_queue_message():
    ce = SimpleCloudEvent()

    now = datetime.utcfromtimestamp(time.time()).replace(tzinfo=timezone.utc)

    ce.time = now

    msg = to_queue_message(ce)

    assert msg
    assert isinstance(msg, bytes)

    ce_msg = json.loads(msg.decode('UTF8'))
    assert isinstance(ce_msg, dict)

    # check that the datetime was serialized to a string correctly
    assert ce_msg['time'] == now.isoformat()
