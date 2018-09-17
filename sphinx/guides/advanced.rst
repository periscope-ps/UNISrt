##############
Advanced Usage
##############

This section covers more nuanced uses of the runtime and special-case examples.

*************************************
The Data Service and Data Collections
*************************************

The :class:`DataCollection <unis.measurements.data.DataCollection>` class provides
features for tracking and managing measurement on
:class:`UnisObjects <unis.models.models.UnisObject>`.
:class:`DataCollections <unis.measurements.data.DataCollection>` are automatically
added to any Metadata type :class:`UnisObject <unis.models.models.UnisObject>`
when they are added to the any runtime with the
:class:`DataService <unis.services.data.DataService>` attached.

:class:`DataCollections <unis.measurements.data.DataCollection>` automatically
begin gathering and collating data streams from the back end data store on creation.
Without a function to analyze the data, however, these measurements are left unused.

In order to use examine data streams, you must first attach a function to the
measurement.  The below code block assumes there is a Runtime object `rt` and
that it contains at least one measurement and that there is a constant stream
of numeric values being added to the measurement from a third party source.

.. code-block:: python
		
    >>> data = rt.metadata[0].data
    >>> data.attachFunction(lambda x, prior: x + prior, name="sum")
    >>> data.sum
    0
    >>> data.sum
    2
    >>> data.sum
    10

The function passed to :meth:`attachFunction <unis.measurements.data.DataCollection.attachFunction>`
must take a new value and a `prior` value, where the `prior` value is the result of the previous
invocation of the function.

For more complex functions, it may be necessary to use the
:class:`Function <unis.measurements.data.Function>` class, which acts as a template for function
interactions.  The simplest form looks the same as the above example, except that it provides
a way of setting the default `prior` value.

.. code-block:: python
		
    >>> from unis.measurements import Function
    >>> data = rt.metadata[0].data
    >>> data.attachFunction(Function(lambda x, prior: x + prior, initial=10), name="sum")
    >>> data.sum
    10
    >>> data.sum
    12
    >>> data.sum
    22

For more complex features, you can inherit from :class:`Function <unis.measurements.data.Function>`
and override the :meth:`Function.apply <unis.measurements.data.Function.apply>` - this serves
the same purpose as passing in a `lambda` function in the above - or
the :meth:`Function.postprocess <unis.measurements.data.Function.postprocess>`
to apply one-time operations on the result before returning to the user; functionality
in :meth:`Function.postprocess <unis.measurements.data.Function.postprocess>` is not
propogated to the next iteration.

.. code-block:: python
		
    >>> class myFunc(Function):
	    def apply(self, x, ts):
	        return x + self.prior
	    def postprocess(self, x)
	        return x * 2
    >>> data.attachFunction(myFunc(initial=5), name='func')
    >>> data.func
    10
    >>> data.func # stream has recieved a 2
    24
    >>> data.func # stream has recieved a 1
    26

As you can see above, the sum is doubled on read, but the ongoing accumulator is left as-is.
This type of behavior is necessary for some streaming functions, such as jitter.  The
function itself is stored as-is within the collection, meaning that you may use
the :class:`Function <unis.measurements.data.Function>` object to store state between
executions.

.. code-block:: python
		
    >>> class window(Function):
            def __init__(self, window_size):
	        self.size = window_size
	        super(Function, self).__init__(initial=[])
            def apply(self, v, ts):
	        self.prior.append(v)
		if len(self.prior) < self.size:
		    return self.prior
		else:
		    return self.prior[self.size:]
    >>> data.attachFunction(window(5))
    >>> data.window
    []
    >>> data.window
    [2]
    >>> data.window
    [2, 10]
    >>> data.window
    [2, 10, 8, 5, 8]
    >>> data.window
    [10, 8, 5, 8, 5]

The above example shows a queue window buffer for a data stream.


***************************
Adding Data to a Collection
***************************

:class:`DataCollections <unis.measurements.data.DataCollection>` are automatically
added to any metadata object inserted into a runtime.  Once they have been linked
with a remote data store, a client can add data to the measurement by using
the :meth:`DataCollections.append <unis.measurements.data.DataCollection.append>`
function.

.. warning::
   Metadata that has been added to a runtime and/or registered to a
   data store but **NOT** linked (flushed), will be in read-only mode
   and cannot be appended to until linked.

.. code-block:: python
		
    >>> from unis.models import Metadata
    >>> subject = rt.links[0]
    >>> data = rt.insert(Metadata({'subject': subject}), commit=True).data
    >>> rt.flush()
    >>> data.append(5)

Notice the `subject` field in the Metadata constructor.  Metadata must include a
subject which is the resource being observed by a measurement.  If the measurement
comes with a timestamp, it may be included in the call by passing the timestamp to
the `ts` field

.. code-block:: python

   >>> data.append(5, ts=timestamp)

