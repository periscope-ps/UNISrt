###############
DataCollections
###############

****************
Data Collections
****************

:class:`DataCollections <unis.measurements.data.DataCollection>` act as centralized curators for a single
measurement actively being collected and stored on a backend data store.  By themselves, they provide
mechanisms for listening for new measurement and creating a backlog of previous measurements.  In order
to expand on that functionality, new :class:`Functions <unis.measurements.data.Function>` must be added
to the :class:`DataCollection <unis.measurements.data.DataCollection>` with the
:meth:`DataCollection.attachFunction <unis.measurements.data.DataCollection.attachFunction>` method.

Attaching a function will register that function as an attribute of the :class:`DataCollection <unis.measurements.data.DataCollection>`.
The attribute name follows the following hierarchy in order:

1. The ``name`` parameter passed in when the :class:`Function <unis.measurements.data.Function>` is created.
2. The *lowercase* name of the :class:`Function <unis.measurements.data.Function>` class.
3. The ``name`` parameter of the :meth:`DataCollection.attachFunction <unis.measurements.data.DataCollection.attachFunction>`.

When accessing the newly created attribute, the value returned will be the result of passing each measurement from the collection
through the :meth:`Function.appy <unis.measurements.data.Function.apply>` function.  Depending on the :class:`Runtime <unis.runtime.runtime.Runtime>`
settings, the actual computation of the above result may happen on attribute access or in real time as measurements are recieved through an open
socket with the data store.

.. autoclass:: unis.measurements.data.DataCollection
   :members:

*********
Functions
*********

.. autoclass:: unis.measurements.data.Function
   :members:

.. autoclass:: unis.measurements.data.Last
   :members:

.. autoclass:: unis.measurements.data.Min
   :members:

.. autoclass:: unis.measurements.data.Max
   :members:

.. autoclass:: unis.measurements.data.Mean
   :members:

.. autoclass:: unis.measurements.data.Jitter
   :members:
