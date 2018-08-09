from unis.measurements import DataCollection
from unis.services.abstract import RuntimeService
from unis.services.event import new_event

class DataService(RuntimeService):
    """
    Automatically creates :class:`DataCollections <unis.measurements.data.DataCollection>`
    for metadata objects for measurement tracking and handling.
    """

    @new_event('metadata')
    def new_md(self, md):
        md.data = DataCollection(md, self.runtime)
