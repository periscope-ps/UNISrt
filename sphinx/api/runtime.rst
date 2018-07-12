################
Runtime
################

***************
Runtime Objects
***************

Multiple instances of the :class:`Runtime <unis.runtime.runtime.Runtime>` class
may be used simultaniously, partitioned logically through namespacing
or due to multiple clients creating their own independent instance.
It is important to understand that the runtime architecture is designed
to minimize object reuse however.  All :class:`Runtime <unis.runtime.runtime.Runtime>`
instances share a common pool of connections to backend data stores.
Even if two separately namespaced instances both independently connect
to the same data store, the same persistent connection will be reused.
This is generally true of any application within the same python process.

Additionally, two :class:`Runtimes <unis.runtime.runtime.Runtime>` sharing the same
namespace will share resource objects.  Changes in one will immediately
reflect in the other.  If this is undesired behavior, make sure to keep :class:`Runtime <unis.runtime.runtime.Runtime>`
instances in distinct namespaces.  This incures a marked space and
performance cost however.

.. autoclass:: unis.runtime.runtime.Runtime
   :members:

************************
Object Abstraction Layer
************************

.. autoclass:: unis.runtime.oal.ObjectLayer
   :members:
