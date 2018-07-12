######
Models
######

************
Unis Objects
************

The :class:`UnisObjects <unis.models.models.UnisObject>` form the core feature set for the runtime.
The :class:`UnisObject <unis.models.models.UnisObject>` classes that applications will interact with
are not the base classes described herein, but instead automatically generated classes that inherit
from :class:`UnisObject <unis.models.models.UnisObject>`.  These classes share all of the functionality
of the base class, as well as several other key features described below:

JSON schema and default properties
""""""""""""""""""""""""""""""""""

Each resource - represented as a JSON document - includes a ``$schema`` field, which contains a
resolveable reference to a JSON Schema document describing the resource's type.  The JSON Schema
is downloaded - if the reference is an href - and cached locally.  This cache is preserved accross
invocations and can be used to run the runtime offline if necessary.

The JSON Schema is then used to generate a ``class`` which inherits from :class:`UnisObject <unis.models.models.UnisObject>`
and a python version of the type defined in the JSON Schema.  Attributes are added from the JSON Schema into the new class.
On construction, these attributes are filled with meaningful default values as defined by the JSON Schema.

:class:`UnisObjects <unis.models.models.UnisObject>` adhere to JSON Schema inheritance, and may be arbitrarily expanded
and enhanced by defining new types in JSON Schema.

Context objects
"""""""""""""""

:class:`Contexts <unis.models.models.Context>` serve as a wrapper class for 
:class:`UnisObjects <unis.models.models.UnisObject>`.  They are transparent
to applications while maintaining context information about the :class:`Runtime <unis.runtime.runtime.Runtime>`
invoking changes to the contained object.
    
:class:`Contexts <unis.models.models.Context>` objects are generated automatically by the runtime in
all of the following cases:

* :class:`UnisObject <unis.models.models.UnisObject>` created via construtor.
* :class:`UnisObject <unis.models.models.UnisObject>` is retrieved from a :meth:`where <unis.models.lists.UnisCollection.where>` or :meth:`find <unis.runtime.oal.ObjectLayer.find>` call.
* :class:`UnisObject <unis.models.models.UnisObject>` retrieved as a property of another :class:`UnisObject <unis.models.models.UnisObject>`
  
In practice, applications never directly touch :class:`UnisObjects <unis.models.models.UnisObject>`, instead they only recieve 
:class:`Contexts <unis.models.models.Context>` linking :class:`UnisObjects <unis.models.models.UnisObject>` to 
:class:`Runtimes <unis.runtime.runtime.Runtime>`.  This distinction is academic in most cases; the :class:`Context <unis.models.models.Context>`
is transparent.

.. note::
   **Special Note** Most functions exposed by :class:`UnisObject <unis.models.models.UnisObject>` include a
   ``ctx`` parameter.  This parameter is inserted automatically by the containing :class:`Context <unis.models.models.Context>`
   when the function is called.  As such, applications must treat the ``ctx`` parameter like the ``self`` parameter and
   **SHOULD NOT** pass in a value to the ``ctx`` parameter for any reason.

Attribute getters
"""""""""""""""""

The primary purpose of the runtime is to allow seamless control over remote objects.  :class:`UnisObject <unis.models.models.UnisObject>`
attributes are responsible for maintaining the coherency between the local cache and remote ground truth.  Reading from an attribute
on a :class:`UnisObject <unis.models.models.UnisObject>` invokes one of four behaviors.  If the attribute is in the form of a
:class:`List <unis.models.models.List>` or :class:`UnisObject <unis.models.models.UnisObject>`
the lookup returns the object as is.  If the attribute is in the form of a non-runtime type, ``list``, ``dict``, ``number``, ``string``,
the value is "lifted" into the corrosponding runtime type.  If the attribute is of the type :class:`Primitive <unis.models.models.Primitive>`,
the raw ``string``, ``number``, or ``boolean`` value is return.  Finally, if the attribute is of type :class:`Local <unis.models.models.Local>`,
the data within the attribute is examined.  In the case that it is a reference to a remote object, that object is located through the
:meth:`ObjectLayer.find <unis.runtime.oal.ObjectLayer.find>` mechanism.  Otherwise, the object is returned as is.

The result of the above transformations is a seamless transition between runtime types and ``buildin`` types where remote objects are cached
on access.

Attribute setters
"""""""""""""""""

When :class:`UnisObject <unis.models.models.UnisObject>` attributes are set to a value, one of two actions takes place.
Each :class:`UnisObject <unis.models.models.UnisObject>` maintains an internal Schema originally constructed from the
JSON Schema describing the type.  Attributes corresponding to this schema are considered *remote* attributes, while those
not on the internal schema are considered *local* attributes.

When a *local* attribute is modified, no action is taken aside from modifing the internal value of the attribute.
Applications may assign to any arbitrary attribute and dynamically adding new attributes is allowed.  However without
using the :meth:`UnisObject.extendSchema <unis.models.models.UnisObject.extendSchema>` function, these attributes will
not be reflected in the corresponding object in the data store.

When a *remote* attribute is modified, the object is marked for update and dispatched to the corresponding :class:`Runtime <unis.runtime.runtime.Runtime>`.
This action **does not** immediately result in a ``POST`` to the backend data store, that is dependent on
the :class:`Runtimes <unis.runtime.runtime.Runtime>` ``defer_update`` setting.  In ``deferred_mode`` pending modifications
will be held until :meth:`ObjectLayer.flush <unis.runtime.oal.ObjectLayer.flush>` is called.  In ``immediate_mode``
marking a resource as pending results in an immediate update to the data store.

=========
Interface
=========

.. data:: unis.models.schemaLoader
   
   Constructor for new :class:`UnisObject <unis.models.models.UnisObject>` classes from JSON Schema.

   .. function:: get_class(schema,name,raw)

      :param str schema: href to the schema used to generate the class.
      :param str name: (optional) name of the class.
      :param bool raw: (optional) Set if class should return :class:`UnisObject <unis.models.models.UnisObject>` or :class:`Context <unis.models.models.Context>`
   
   Build a :class:`UnisObject <unis.models.models.UnisObject>` class from a JSON Schema.
      

.. autoclass:: unis.models.models.Context
   :members:

.. autoclass:: unis.models.models.UnisObject
   :members:

*******************
Subordinant Objects
*******************

In order to represent data stored in :class:`UnisObjects <unis.models.models.UnisObject>`, the runtime uses the following
classes to represent objects, lists, numbers, and strings while maintaining internal bookkeeping.

.. warning:: All of the following classes are used to store data internally within :class:`UnisObjects <unis.models.models.UnisObject>`.
	     They should not be used directly by client programs.

.. autoclass:: unis.models.models.Local
   :members:

.. autoclass:: unis.models.models.List
   :members:

.. autoclass:: unis.models.models.Primitive
   :members:

.. autoclass:: unis.models.models.SkipResource
   :members:
