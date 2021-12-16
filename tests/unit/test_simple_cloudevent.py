
import json
import time
from datetime import datetime, timezone

from freezegun import freeze_time

from simple_cloudevent import SIMPLE_CE_SPEC_VERSION, SIMPLE_CE_SPEC_CONTENTTYPE
from simple_cloudevent import SimpleCloudEvent
from simple_cloudevent import to_structured


def test_empty_simple_cloudevent():
    ce = SimpleCloudEvent()

    assert ce.version == SIMPLE_CE_SPEC_VERSION
    assert ce.datacontenttype == SIMPLE_CE_SPEC_CONTENTTYPE


def test_to_structured_for_simple_cloudevent():
    ce = SimpleCloudEvent()

    now = datetime.utcfromtimestamp(time.time()).replace(tzinfo=timezone.utc)

    with freeze_time(now):
        ce_dict = to_structured(ce)

    # check to see the defaults are applied and that the dict was created
    assert isinstance(ce_dict, dict)
    assert ce_dict['time'] == now.isoformat()
    assert ce_dict['id']
    assert not ce_dict.get('data')


def test_to_structured_with_update_for_simple_cloudevent():
    ce = SimpleCloudEvent()

    now = datetime.utcfromtimestamp(time.time()).replace(tzinfo=timezone.utc)

    with freeze_time(now):
        ce_dict = to_structured(ce, True)

    # check to see the defaults are applied and that the dict was created
    assert isinstance(ce_dict, dict)
    assert ce_dict['time'] == ce.time
    assert ce_dict['id'] == ce.id
    assert not ce_dict.get('data')
