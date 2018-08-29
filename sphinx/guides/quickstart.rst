##########
Quickstart
##########


******************
Connecting to UNIS
******************

Creating a runtime object and connecting to a unis instance can be done a few
different ways.  The simplest is passing a single url to the Runtime
constructor to the root address of a UNIS instance.

.. code-block:: python

   >>> from unis import Runtime
   >>> rt = Runtime('http://localhost:8888')

Alternatively, the runtime can track multiple instances simultaniously.  This
process is automatic if a new instance is discovered in the course of operation;
for instance if a document references a UNIS instance not currently tracked by
the runtime, it will add it to it's known endpoints and begin tracking it.
Instead, you can pass a list of endpoints to the constructor to initailize with
an entire set of instances at the same time.

If a list of urls is provided, the first reference is assumed to be the
*default* instance; any new documents will be pushed to the *default* instance
unless otherwise indicated.

.. code-block:: python
   
   >>> rt = Runtime(['http://localhost:8888', 'http://localhost:9000'])

Finally, any simple url can be replaced with a python dictionary describing the
remote instance.

.. code-block:: python
   
   >>> rt = Runtime({'url': 'http://localhost:8888', 'default': True,
		     'verify': False, 'ssl': None, 'enabled': True })

The dictionary form of a UNIS reference follows the below schema:
 * **url**: The reference to the instance.
 * **default**: Indicates whether an instance should be used by the runtime when
   no other instance is given.
 * **verify**: Requires that SSL certificates be verified.
 * **ssl**: Reference to the SSL certificate file to use with the connection.
 * **enabled**: Use to disable an instance manually (more useful for application
   configuration).

***************
Using Resources
***************

Resources are objects that are compatable with the runtime.  They provide
default values generated from json schemas and read/write access to properties.

.. code-block:: python

   >>> from unis.models import Node
   >>> n = Node()
   >>> n.name
   ''
   >>> n.status
   'UNKNOWN'

Resources can be instantiated with a set of `key:value` pairs by
passing in a dictionary.

.. code-block:: python

   >>> n = Node({'name': 'mynode', 'foo': 5})
   >>> n.name
   'mynode'
   >>> n.foo
   5
   >>> n.foo = 10
   >>> n.foo
   10


****************
Resource queries
****************

Resources are stored in *collections*.  Each collection is a property of the
runtime object.  These collections are generated whenever a new UNIS instance
is connected to the runtime (including during initialization).

You can find a list of the currently loaded collections with the
:meth:`Runtime.about <unis.runtime.oal.ObjectLayer.about>` function.

.. code-block:: python

   >>> rt = Runtime('http://localhost:8888')
   >>> rt.about()
   ['extents', 'paths', 'metadata', 'nodes', 'domains', 'services',
   'topologies', 'measurements', 'exnodes', 'networks', 'links', 'ports']

*Collections* have list-like behaviors:

.. code-block:: python
   
   >>> n = rt.nodes[0]
   >>> n.id
   '13d87bff-3675-4c98-8f11-17b952025b4d'
   >>> for n in rt.nodes:
           print(n.name)
   'A'
   'B'
   'C'

The above code assumes that `http://localhost:8888` contains three
nodes, with names "A", "B", and "C" respectively.

You can also query collections with the
:meth:`UnisCollection.where <unis.models.lists.UnisCollection.where>` function.
This can take a dictionary, containing `key:value` pairs to match.
:meth:`UnisCollection.where <unis.models.lists.UnisCollection.where>` returns
a generator which will produced all of the resources matching the query.

.. code-block:: python
   
   >>> for n in rt.nodes.where({'name': 'A'}):
           n.name
   'A'

In addition to literal equivolence, the dictionary form of
:meth:`UnisCollection.where <unis.models.lists.UnisCollection.where>`
can also take simple logical comparisions.

.. code-block:: python
   
   >>> for n in rt.nodes.where({'name': {'gt': 'B'}}):
           n.name
   'C'

Queries can also be in the form of a filter function:

.. code-block:: python
   
   >>> for n in rt.nodes.where(lambda x: x.name == 'A'):
           n.name
   'A'

The filter form of
:meth:`UnisCollection.where <unis.models.lists.UnisCollection.where>` is slower
and should only be used in cases where your filter condition cannot be written
in the form of a match dictionary.

Finally, for cases where only a single instance is needed, collections provide
a :meth:`first_where <unis.models.lists.UnisCollection.first_where>` function.

.. code-block:: python

   >>> n = rt.nodes.first_where({'name': 'A'})
   >>> n.name
   'A'

   
********************
Adding new resources
********************

Resources exist in one of three states:

* **Detatched**: Newly created resource not belonging to a runtime.
* **Registered**: Resource has been added to a runtime, but only exists within the runtime (it will not be pushed to a remote data store).
* **Linked**: Resource is linked to a remote data store document.

These three states form a strictly linear relationship.  A resource MUST be
**detatched** to be **registered**, and a resource MUST be **registered** to
be **linked** to a remote document.  This relationship is also directional,
you cannot devolve a **linked** resource to a **registered** resource without
deleting the resource and building a new one.

.. code-block:: python

   >>> n = Node()
   >>> rt.insert(n)
   <unis.models.models.Node dict_keys(['_rt_live', 'status', 'selfRef',
   'description', '$schema', 'id', 'ports', 'urn', 'name', 'rules', 'ts',
   'lifetimes', 'relations', 'location', 'properties'])>

`n` in the above snippet is **detatched** in line 1, and **registered** in line 2.
Calling :meth:`UnisObject.commit <unis.models.models.UnisObject.commit>`
on `n` will **link** it to a record on the *default* UNIS instance.  Note that
if a record does not exist for an object being linked, one will be created.

.. code-block:: python
   
   >>> n.commit()

You can jump straight from **detatched** to **linked** by using the `commit`
parameter of the :meth:`insert <unis.models.lists.UnisCollection.insert>`
function.

.. code-block:: python
   
   >>> rt.insert(n, commit=True)

******************************
Manipulating remote properties
******************************

Once a resource has been **linked**, modifications to the resource will be
staged for insertion to the data store.  This is only true for properties
included in the resources **schema**.

"""""""""""""""
Resource Schema
"""""""""""""""

Each resource maintains an internal schema of what the object should look like
according to the remote data store.  This allows the user to add annotations
to the object without pushing them to the remote resource.  The
:meth:`UnisObject.to_JSON <unis.models.models.UnisObject.to_JSON>` function
returns a python dictionary stored in the remote data store.

.. code-block:: python
   
   >>> from unis.models import Node
   >>> from unis import Runtime
   >>> rt = Runtime('http://localhost:8888')
   >>> n = Node()
   >>> rt.insert(n, commit=True)
   >>> n.to_JSON()
   {'status': 'UNKNOWN',
    'selfRef': 'http://localhost:8888/nodes/7fc1a6d3-5d26-457c-b63e-014a95cd378e',
    '$schema': 'http://unis.crest.iu.edu/schema/20160630/node#',
    'location': {}, 'ports': [], 'properties': {}, 'urn': '', 'name': '',
    'rules': [], 'ts': 0, 'lifetimes': [], 'relations': {},
    'id': '7fc1a6d3-5d26-457c-b63e-014a95cd378e', 'description': ''}
   >>> n.foo = "This is a note"
   >>> n.to_JSON()
   {'status': 'UNKNOWN',
    'selfRef': 'http://localhost:8888/nodes/7fc1a6d3-5d26-457c-b63e-014a95cd378e',
    '$schema': 'http://unis.crest.iu.edu/schema/20160630/node#',
    'location': {}, 'ports': [], 'properties': {}, 'urn': '', 'name': '',
    'rules': [], 'ts': 0, 'lifetimes': [], 'relations': {},
    'id': '7fc1a6d3-5d26-457c-b63e-014a95cd378e', 'description': ''}

Notice that the output is the same even after adding `foo` to `n`.  `n` does
contain a property `foo`, it is just hidden from the remote data store.

The resource **schema** is formed of a combination of the json schema used
to define the resources' type, properties passed in during instantiation,
and by use of the
:meth:`UnisObject.extendSchema <unis.models.models.UnisObject.extendSchema>`
funcion.

.. code-block:: python
   
   >>> n = Node()
   >>> list(n.to_JSON().keys())
   ['status', 'selfRef', '$schema', 'location', 'ports', 'properties', 'urn',
   'name', 'rules', 'ts', 'lifetimes', 'relations', 'id', 'description']
   >>> n = Node({'foo': 5})
   ['status', 'foo', 'selfRef', '$schema', 'location', 'ports', 'properties',
   'urn', 'name', 'rules', 'ts', 'lifetimes', 'relations', 'id', 'description']
   >>> n.extendSchema('bar', 5)
   ['bar', 'status', 'foo', 'selfRef', '$schema', 'location', 'ports',
   'properties', 'urn', 'name', 'rules', 'ts', 'lifetimes', 'relations', 'id',
   'description']

""""""""""""""""""""""""""""
Deferred and Immediate Modes
""""""""""""""""""""""""""""

Up to this point, we have said only that the changes will be *staged*.  That is
because the exact behavior of the document update depends on the configuration
of the runtime.  If the runtime is in "deferred mode" (default), changes are
only staged within the runtime until either the runtime is closed or the
:meth:`Runtime.flush <unis.runtime.oal.ObjectLayer.flush>` function is called.
Either of these with cause all pending changes to be collected and pushed to
the remote data stores.

Both of the below examples result in the resource being pushed to the data
store.

.. code-block:: python

   >>> n = rt.insert(Node(), commit=True)
   >>> n.name = "Example"
   >>> rt.flush()

In immediate mode, staged changes are pushed as soon as they are made, this
incurs more overhead, but is also more responsive to minute changes.

.. code-block:: python

   >>> rt = Runtime({'poxy': {'defer_update': False}})
   >>> n = rt.insert(Node(), commit=True)
   >>> n.name = "Example"


****************
Adding a service
****************

*Under construction*
