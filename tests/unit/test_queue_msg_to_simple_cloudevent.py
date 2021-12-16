


import json
import time
from datetime import datetime, timezone

import pytest
from freezegun import freeze_time

from simple_cloudevent import SIMPLE_CE_SPEC_VERSION, SIMPLE_CE_SPEC_CONTENTTYPE
from simple_cloudevent import SimpleCloudEvent
from simple_cloudevent import to_structured
from simple_cloudevent import to_queue_message
from simple_cloudevent import from_queue_message


def test_que_msg_to_ce():
    msg = b'{"datacontenttype": "application/json", "id": "6f589627-448c-4586-85b3-fca406aacf1b", "source": null, "specversion": "1.0", "subject": null, "time": "2021-12-16T06:59:11.135685+00:00", "type": null}'
    
    ce = from_queue_message(msg)

    assert ce
    assert isinstance(ce, SimpleCloudEvent)

    with pytest.raises(Exception) as excinfo:
        msg = b'{"datacontenttype": "application/json", "id": "6f589627-448c-4586-85b3-fca406aacf1b", "source": null, "specversion": "0.0", "subject": null, "time": "2021-12-16T06:59:11.135685+00:00", "type": null}'
        ce = from_queue_message(msg)

    val = str(excinfo.value)
    assert 'Spec Version mismatch, expected 1.0 got 0.0' in str(excinfo.value)
