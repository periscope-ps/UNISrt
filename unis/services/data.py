from unis.models import Metadata
from unis.measurements import DataCollection
from unis.services.abstract import RuntimeService

class DataService(RuntimeService):
    targets = [Metadata]
    def new(self, resource):
        resource.data = DataCollection(resource.selfRef, self.runtime)
