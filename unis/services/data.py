
from unis.services.abstract import RuntimeService
from unis.models import Metadata
from unis.models.lists import DataCollection

class DataService(RuntimeService):
    def new(self, resource):
        if "Metadata" in resource.names:
            resource.data = DataCollection(resource.id, self.runtime)
            
