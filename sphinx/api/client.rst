#######################
Managing Remote Clients
#######################
The runtime maintains persistent connections to one or more remote datastores.
These connections are maintained and controlled using the :class:`UnisProxy <unis.rest.unis_client.UnisProxy>`
and :class:`UnisClient <unis.rest.unis_client.UnisClient>` classes.  The former represents the collection of
connections for a specific class of object, while the later maintains the persistent
connection to a specific endpoint.

The :class:`UnisClient <unis.rest.unis_client.UnisClient>` class is a form of singleton; each remote data store
has one *and only one* :class:`UnisClient <unis.rest.unis_client.UnisClient>` associated with it.  The singleton
relationship is maintained through the :class:`ClientID <unis.rest.unis_client.CID>` class which represetns a unique
ID generated and advertised by the remote data store.  :class:`ClientIDs <unis.rest.unis_client.CID>` are FQDN agnostic
i.e. a :class:`UnisClient <unis.rest.unis_client.UnisClient>` generated from ``http://localhost:8888`` and ``http://192.168.0.1:8888``
will resolve to the same instance given that ``localhost`` resolves to ``192.168.0.1``.

.. warning::
   The classes described herein are intended for internal runtime use only, these documents are included
   for completeness and development documentation only.


.. automodule:: unis.rest.unis_client
   :members:

.. autoclass:: UnisProxy
   :members:

.. autoclass:: UnisClient
   :members:
