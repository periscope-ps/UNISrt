import json, requests

from lace.logging import trace

from unis.measurements import DataCollection
from unis.models import Metadata, Data, Node
from unis.rest import UnisClient
from unis.services.abstract import RuntimeService
from unis.services.event import new_event, commit_event, postflush_event

@trace("unis.services")
class DataService(RuntimeService):
    """
    Automatically creates :class:`DataCollections <unis.measurements.data.DataCollection>`
    for metadata objects for measurement tracking and handling.
    """
    
    @commit_event('metadata')
    def add_md(self, md):
        md.data = DataCollection(md, self.runtime)
        md.data.read_only = True

    @postflush_event('metadata')
    def add_event(self, md):
        if md.data.read_only:
            event = {'metadata_URL': md.selfRef, 'collection_size': 100000, 'ttl': 1500000}
            source = UnisClient.instances[md.getSource()].synchronous_post('events', event)
            md.data.read_only = False

    @new_event('metadata')
    def new_md(self, md):
        if md.selfRef:
            md.data = DataCollection(md, self.runtime)
