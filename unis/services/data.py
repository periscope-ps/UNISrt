from unis.models import Metadata
from unis.measurements import DataCollection
from unis.services.abstract import RuntimeService

class DataService(RuntimeService):
    """
    Automatically creates :class:`DataCollections <unis.measurements.data.DataCollection>`
    for metadata objects for measurement tracking and handling.
    """
    targets = [Metadata]
    def new(self, resource):
        resource.data = DataCollection(resource.selfRef, self.runtime)
