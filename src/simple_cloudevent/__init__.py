# Copyright 2021 Daxiom Systems Ltd. All Rights Reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
"""Simple CloudEvent library."""

from __future__ import annotations

import json
import time as _time
import uuid
from dataclasses import asdict, dataclass, field
from datetime import date, datetime, timezone
from typing import Final, MutableMapping, Optional, Union

import strict_rfc3339

from .exception import CloudEventVersionException, InvalidCloudEventError
from .__version__ import __version__


SIMPLE_CE_SPEC_VERSION: Final = '1.0'
SIMPLE_CE_SPEC_CONTENTTYPE: Final = 'application/json'


TEMPLATE_CE = {
    'data': {},
    'datacontenttype': SIMPLE_CE_SPEC_CONTENTTYPE,
    'id': None,
    'source': None,
    'specversion': SIMPLE_CE_SPEC_VERSION,
    'subject': None,
    'time': None,
    'type': None,
}


def _json_serial(obj):
    '''JSON serializer for datetime and dates.'''
    if isinstance(obj, (datetime, date)):
        return obj.isoformat()
    raise TypeError(f'Type {type(obj)} not serializable')


def to_structured(ce: SimpleCloudEvent, update: bool = False) -> dict:
    '''Create a valid dict from the SimpleCloudEvent.

    set the defaults appropriately.

    setting the update flag to True, updates the SimpleCloudEvents for any mssing values.

    id: if None, gets set to a str(uuid)
    time: if None, gets an isoformated string of a timestamp created at conversion time.'''

    if ce.time and isinstance(ce.time, str) and not strict_rfc3339.validate_rfc3339(ce.time):
        raise ValueError('The time field must be in strict ISO 8601 format.')

    ce_dict = {**TEMPLATE_CE, **asdict(ce)}

    ce_dict['id'] = ce_dict['id'] or str(uuid.uuid4())
    if update:
        ce.id = ce_dict['id']

    # although the spec allows for NULL time, we put in a time
    ce_dict['time'] = ce_dict['time'] \
        or datetime.utcfromtimestamp(_time.time()).replace(tzinfo=timezone.utc).isoformat()
    if update:
        ce.time = ce_dict['time']

    if not ce_dict['data'] or ce_dict['data'] == {}:
        ce_dict.pop('data', None)

    return ce_dict


def to_queue_message(ce: SimpleCloudEvent) -> bytes:
    '''Return a byte string, of the CloudEvent in JSON format.'''
    return json.dumps(to_structured(ce), default=_json_serial).encode('UTF8')


def from_queue_message(msg: bytes) -> SimpleCloudEvent:
    '''Convert a queue message back to a simple CloudEvent.'''
    msg_dict = json.loads(msg.decode('UTF8'))

    if not isinstance(msg_dict, MutableMapping):
        raise InvalidCloudEventError('The message must be a dict.')

    msg_dict.pop('datacontenttype', None)

    if (spec := msg_dict.pop('specversion', None)) != SIMPLE_CE_SPEC_VERSION:
        raise CloudEventVersionException(f'Spec Version mismatch, expected {SIMPLE_CE_SPEC_VERSION} got {spec}')

    try:
        ce = SimpleCloudEvent(**msg_dict)
    except Exception as e:
        raise InvalidCloudEventError(f'Invalid CloudEvent. {e}', e)

    return ce


@dataclass
class SimpleCloudEvent:
    '''Holds the data for a simple CloudEvent.'''

    id: Optional[str] = None
    source: Optional[str] = None
    subject: Optional[str] = None
    time: Union[datetime, str, None] = None
    type: Optional[str] = None
    data: dict = field(default_factory=dict)

    @classmethod
    @property
    def specversion(cls):
        '''Return the version of the CloudEvent spec.'''
        return cls.version

    @classmethod
    @property
    def version(cls):
        '''Return the version of the CloudEvent spec.'''
        return SIMPLE_CE_SPEC_VERSION

    @classmethod
    @property
    def datacontenttype(cls):
        '''Return the JSON content type, as the only type supported.'''
        return SIMPLE_CE_SPEC_CONTENTTYPE
