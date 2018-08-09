########
Services
########

****************
Runtime Services
****************
 
:class:`RuntimeServices <unis.services.abstract.RuntimeService>` provide event driven functionality for
applications.  By defining a service and attaching it to a runtime with :meth:`Runtime.addService <unis.runtime.runtime.Runtime.addService>`
or by adding the service to the initial :class:`Runtime <unis.runtime.runtime.Runtime>` parameters applications can subscribe to ``new``,
``update``, and ``delete`` events by collection.

:class:`RuntimeServices <unis.services.abstract.RuntimeService>` allow for more fine coarse control than
:meth:`UnisCollection.addCallback <unis.models.lists.UnisCollection.addCallback>` and
:meth:`UnisObject.addCallback <unis.models.lists.UnisCollection.addCallback>`, but groups operations into a single logical class.

To create a service, write a class that inherits from :class:`RuntimeService <unis.services.abstract.RuntimeService>`.  Any function within
the inheriting class that uses the decorators described in the :mod:`unis.services.event <unis.services.event>` module will be automatically
registered as callbacks for the appropriate collection and event.

=========
Interface
=========

.. autoclass:: unis.services.abstract.RuntimeService
   :members:
   
.. autoclass:: unis.services.abstract.ServiceMetaclass
   :members:

.. autoclass:: unis.services.graph.UnisGrapher
   :members:

.. autoclass:: unis.services.data.DataService
   :members:

******
Events
******

.. automodule:: unis.services.event
   :members:
   :exclude-members: Event

******************
Supporting classes
******************

.. autoclass:: unis.services.graphbuilder.Graph
   :members:
