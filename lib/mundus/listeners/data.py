from unis.models import Metadata
from unis.measurements import DataCollection

from unis.events import create_event, update_event, publish_event, postpush_event

@publish_event(Metadata)
def mark_data_as_readonly(md):
    md.subject.get_measurement(md.eventType).registered = False

@create_event(Metadata)
@update_event(Metadata)
def attach_data_to_objects(md):
    try: md.subject.get_measurement(md.eventType)
    except KeyError: data = md.subject.add_measurement(md.eventType, md)
    except AttributeError: pass

@postpush_event(Metadata)
def create_event_pool(md):
    if not md.subject.get_measurement(md.eventType).registered:
        event = {'metadata_URL': md.selfRef, 'collection_size': 100000, 'ttl': 1500000}
        md.get_container()._client.post('events', event)
        md.subject.get_measurement(md.eventType).registered = True
