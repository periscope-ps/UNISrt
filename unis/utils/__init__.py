import bisect
import functools

from lace.logging import trace

from unis.utils.pubsub import Events
from unis.exceptions import CollectionIndexError

class _keyblock(object):
    def __init__(self):
        self._ls, self._next, self._prev = [], [], []

    def append(self, v):
        self._ls.append(v)
    def remove(self, v):
        self._ls.remove(v)
    def __contains__(self, v):
        return v in self._ls
    def __iter__(self):
        if self._ls != "TERM":
            for v in self._ls:
                yield v
            if self._next:
                for v in self._next:
                    yield v
    def __reversed__(self):
        if self._ls != "TERM":
            for v in reversed(self._ls):
                yield v
            if self._prev:
                for v in reversed(self._prev):
                    yield v

class Index(object):
    """
    :param str key: Key name for the index.
    
    :class:`Index <unis.utils.Index>` maintains a single index over a single key for
    all resources in a :class:`UnisCollection <unis.models.lists.UnisCollection>`.  
    This index assists the :class:`UnisCollection <unis.models.lists.UnisCollection>` on
    resource lookup in sub-linear time.
    """
    @trace.debug("Index")
    def __init__(self, key):
        self.key = key
        self._head = _keyblock()
        self._head._ls = "TERM"
        self._head._prev, self._head._next = self._head, self._head
        self._block_keys = []
        self._blocks = {}
        self._reverse = {}

    @trace.info("Index")
    def index(self, v):
        """
        :param any v: Value to lookup in the index.
        :returns: A set of `int` indices.
        
        Returns a set of indices associated with resources that contain
        a value `v` in the field associated with the :class:`Index <unis.utils.Index>`.
        """
        return self.subset("eq", v)
    @trace.info("Index")
    def subset(self, comp, v):
        """
        :param str comp: Comparitor to use over the :class:`Index <unis.utils.Index>`.
        :param any v: Value to compare.
        :returns: A set of `int` indices.
        
        Returns a set of indices associated with resources that fulfil the comparitor
        in relation to the value `v`.  `comp` must be in [`gt`, `ge`, `lt`, `le`, `eq`, `in`].
        In all cases except `in`, `v` is a single value to compare to the value of each resource
        in the field corresponding to the :class:`Index <unis.utils.Index>`.  In the case of `in`,
        `v` is a list of values where the returned set includes all resources with a field value
        contained in `v`.
        """
        if comp == 'in':
            return set().union(*[self.subset('eq', x) for x in v])
        block = self._get_block(v)
        slices = {
            "gt": lambda: set(block._next),
            "ge": lambda: set(block),
            "lt": lambda: set(reversed(block._prev)),
            "le": lambda: set(reversed(block)),
            "eq": lambda: set(block._ls),
        }
        return slices[comp]()
    
    @trace.info("Index")
    def update(self, index, value):
        """
        :param int index: Position of the resource in the collection.
        :param any value: Value of the field in the resource.
        
        Takes a given index and associates it with a specified value in the
        :class:`Index <unis.utils.Index>`.
        """
        try:
            self.remove(index)
        except CollectionIndexError:
            pass
        block = self._get_block(value)
        block.append(index)
        self._reverse[index] = value
    @trace.info("Index")
    def remove(self, index):
        """
        :param int index: Position of the resource in the collection.
        :raises CollectionIndexError: If the resource does not exist in the :class:`Index <unis.utils.Index>`
        
        Removes a resource index from the :class:`Index <unis.utils.Index>`
        """
        if index not in self._reverse:
            raise CollectionIndexError("Cannot remove resource from index_{}".format(self.key))
        block_name = self._reverse[index]
        block = self._blocks[block_name]
        
        del self._reverse[index]
        block.remove(index)
        if not block._ls:
            block._prev._next = block._next
            block._next._prev = block._prev
            self._block_keys.remove(block_name)
            del self._blocks[block_name]
            del block
    @trace.debug("Index")
    def _get_block(self, k):
        if k in self._blocks:
            return self._blocks[k]

        new_block = _keyblock()
        i = bisect.bisect_left(self._block_keys, k)
        next_ = self._blocks[self._block_keys[i]] if i < len(self._block_keys) else self._head
        new_block._prev, new_block._next = next_._prev, next_
        new_block._prev._next = new_block._next._prev = new_block
        self._block_keys.insert(i, k)
        self._blocks[k] = new_block
        return new_block

    def __repr__(self):
        pairs = ", ".join(["{}={}".format(k, ",".join(map(str, self._blocks[k]._ls))) for k in self._block_keys])
        return "<Index [{}]>".format(pairs)
        
class UniqueIndex(object):
    """
    :param str key: Key name for the index.
    
    :class:`UniqueIndex <unis.utils.UniqueIndex>` maintains a single index over a single key for
    all resources in a :class:`UnisCollection <unis.models.lists.UnisCollection>`.  
    This index assists the :class:`UnisCollection <unis.models.lists.UnisCollection>` on
    resource lookup in sub-linear time. Unlike :class:`Index <unis.utils.Index>`,
    :class:`UniqueIndex <unis.utils.UniqueIndex>` entries *must* be unique.  In exchange,
    lookup is faster.
    """
    @trace.debug("UniqueIndex")
    def __init__(self, key):
        self.key = key
        self._index = {}
        self._reverse = {}
    @trace.info("UniqueIndex")
    def index(self, v):
        """
        :param any v: Value to lookup in the index.
        :returns: An `int` index position for the resource with the associated value.
        :raises CollectionIndexError: If no resource has the requested value.
        
        Returns the index of the associated resource with a value `v` 
        in the field associated with the :class:`UniqueIndex <unis.utils.UniqueIndex>`.
        """
        if v not in self._index:
            raise CollectionIndexError("Value not in index_{} - {}".format(self.key, v))
        return self._index[v]
    @trace.info("UniqueIndex")
    def subset(self, comp, v):
        """
        :param str comp: Comparitor to use over the :class:`Index <unis.utils.Index>`.
        :param any v: Value to compare.
        :returns: An `int` index position for the resource with the associated value.
        :raises CollectionIndexError: If an incompatable comparitor is requested or index does not exist.
        
        Functions as :meth:`UniqueIndex.index <unis.utils.UniqueIndex.index>` when `comp` is
        given `eq`.  For :class:`UniqueIndices <unis.utils.UniqueIndex>`, only `eq` is a 
        valid comparitor.
        """
        if comp != "eq":
            raise CollectionIndexError("Unique indices can only be queried over equivalence")
        return set([self.index(v)])
    @trace.info("UniqueIndex")
    def update(self, index, value):
        """
        :param int index: Position of the resource in the collection.
        :param any value: Value of the field in the resource.
        
        Takes a given index and associates it with a specified value in the
        :class:`UniqueIndex <unis.utils.UniqueIndex>`.
        """
        if value in self._index and self._index[value] != index:
            raise CollectionIndexError("index_{} conflict - {}".format(self.key, value))
        self._reverse[index] = value
        self._index[value] = index
    @trace.info("UniqueIndex")
    def remove(self, index):
        """
        :param int index: Position of the resource in the collection.
        :raises CollectionIndexError: If the resource does not exist in the :class:`Index <unis.utils.Index>`
        
        Removes a resource index from the :class:`Index <unis.utils.Index>`
        """
        if index not in self._reverse:
            raise CollectionIndexError("Cannot remove resource from index_{}".format(self.key))
        v = self._reverse[index]
        del self._reverse[index]
        del self._index[v]
