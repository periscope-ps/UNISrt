########
Services
########

****************
Runtime Services
****************
 
:class:`RuntimeServices <unis.services.abstract.RuntimeService>` provide event driven functionality for
applications.  By defining a service and attaching it to a runtime with :meth:`ObjectLayer.addService <unis.runtime.oal.ObjectLayer.addService>`
applications can subscribe to ``new``, ``update``, and ``delete`` events by collection.

:class:`RuntimeServices <unis.services.abstract.RuntimeService>` allow for more coarse grain control than :meth:`UnisCollection.addCallback <unis.models.lists.UnisCollection.addCallback>`
and :meth:`UnisObject.addCallback <unis.models.lists.UnisCollection.addCallback>`, but groups operations into a single logical class.

To create a service, write a class that inherits from :class:`RuntimeService <unis.services.abstract.RuntimeService>` and override any of
the :meth:`RuntimeService.new <unis.services.abstract.RuntimeService.new>`, :meth:`RuntimeService.update <unis.service.abstract.RuntimeService.update>`, and 
:meth:`RuntimeService.delete <unis.service.abstract.RuntimeService.delete>` functions.  Modifying the :attr:`targets <unis.services.abstract.RuntimeService.targets>`
``list`` allows custom services to choose which collections to subscribe to.

.. autoclass:: unis.services.abstract.RuntimeService
   :members:
   
   .. automethod:: unis.services.abstract.RuntimeService.new
   
        :param resource: New resource created.
        :type resource: :class:`UnisObject <unis.models.models.UnisObject>`
        
        Abstract function ``new`` is called when a new resource within the collections listed in the :attr:`targets <unis.services.abstract.RuntimeService.targets>`
        attribute is created.

   .. automethod:: unis.services.abstract.RuntimeService.update

        :param resource: Resource updated.
        :type resource: :class:`UnisObject <unis.models.models.UnisObject>`
        
        Abstract function ``update`` is called when a resource within the :attr:`targets <unis.services.abstract.RuntimeService.targets>`
        attribute is modified.

   .. automethod:: unis.services.abstract.RuntimeService.delete

        :param resource: Resource deleted.
        :type resource: :class:`UnisObject <unis.models.models.UnisObject>`
        
        Abstract function ``delete`` is called when a resource within the :attr:`targets <unis.services.abstract.RuntimeService.targets>`
        attribute is deleted.

.. autoclass:: unis.services.abstract.ServiceMetaclass
   :members:

.. autoclass:: unis.services.graph.UnisGrapher
   :members:

   .. automethod:: unis.services.graph.UnisGrapher.new
 		   
        :param resource: New resource
        
        Adds ``nodes`` to the :class:`Graph <unis.services.graphbuilder.Graph>` object as verticies.
        Adds ``links`` to the :class:`Graph <unis.services.graphbuilder.Graph>` object as edges.
   
   .. automethod:: unis.services.graph.UnisGrapher.update
 		   
        :param resource: Resource updated
        
        Adds ``nodes`` to the :class:`Graph <unis.services.graphbuilder.Graph>` object as verticies.
        Adds ``links`` to the :class:`Graph <unis.services.graphbuilder.Graph>` object as edges.
   
.. autoclass:: unis.services.data.DataService
   :members:

   .. automethod:: unis.services.data.DataService.new

      :param resource: New Resource.

      On metadata type object creation, creates a :class:`DataCollection <unis.measurements.data.DataCollection>` and
      associates it with the new metadata object.
   
******************
Supporting classes
******************

.. autoclass:: unis.services.graphbuilder.Graph
   :members:
