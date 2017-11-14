
from unis.services.abstract import RuntimeService
from unis.models import Metadata
from unis.models.lists import DataCollection

class DataService(RuntimeService):
    targets = [Metadata]
    def new(self, resource):
        resource.data = DataCollection(resource.selfRef, self.runtime)
