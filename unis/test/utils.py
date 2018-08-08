import unittest

from unis.exceptions import CollectionIndexError
from unis.utils import Index, UniqueIndex

class IndexTest(unittest.TestCase):
    def _basic_index(self):
        index = Index('test')
        index.update(0, 'a')
        index.update(1, 'b')
        index.update(2, 'c')
        
        return index
    
    def test_create_index(self):
        #setup
        index = Index('test')

        self.assertEqual(index.key, 'test')

    def test_update_insert(self):
        index = Index('test')

        index.update(0, 'a')

        self.assertEqual(index.index('a'), set([0]))

    def test_index_get(self):
        index = self._basic_index() # 0=a, 1=b, 2=c

        self.assertEqual(index.index('a'), set([0]))
        self.assertEqual(index.index('b'), set([1]))
        self.assertEqual(index.index('c'), set([2]))
        self.assertEqual(index.index('d'), set([]))

    def test_index_gt(self):
        index = self._basic_index() # 0=a, 1=b, 2=c

        self.assertEqual(index.subset('gt', 'b'), set([2]))
        self.assertEqual(index.subset('gt', 'a'), set([1, 2]))
        self.assertEqual(index.subset('gt', 'c'), set([]))

    def test_index_ge(self):
        index = self._basic_index() # 0=a, 1=b, 2=c

        self.assertEqual(index.subset('ge', 'b'), set([1, 2]))
        self.assertEqual(index.subset('ge', 'a'), set([0, 1, 2]))
        self.assertEqual(index.subset('ge', 'c'), set([2]))
        
    def test_index_lt(self):
        index = self._basic_index() # 0=a, 1=b, 2=c
        
        self.assertEqual(index.subset('lt', 'b'), set([0]))
        self.assertEqual(index.subset('lt', 'a'), set([]))
        self.assertEqual(index.subset('lt', 'c'), set([0, 1]))
        
    def test_index_le(self):
        index = self._basic_index() # 0=a, 1=b, 2=c

        self.assertEqual(index.subset('le', 'b'), set([0, 1]))
        self.assertEqual(index.subset('le', 'a'), set([0]))
        self.assertEqual(index.subset('le', 'c'), set([0, 1, 2]))
        
    def test_index_eq(self):
        index = self._basic_index() # 0=a, 1=b, 2=c

        self.assertEqual(index.subset('eq', 'b'), set([1]))
        self.assertEqual(index.subset('eq', 'a'), set([0]))
        self.assertEqual(index.subset('eq', 'c'), set([2]))
        
    def test_index_in(self):
        index = self._basic_index() # 0=a, 1=b, 2=c

        self.assertEqual(index.subset('in', []), set([]))
        self.assertEqual(index.subset('in', ['a']), set([0]))
        self.assertEqual(index.subset('in', ['b']), set([1]))
        self.assertEqual(index.subset('in', ['c']), set([2]))
        self.assertEqual(index.subset('in', ['a', 'b']), set([0, 1]))
        self.assertEqual(index.subset('in', ['a', 'c']), set([0, 2]))
        self.assertEqual(index.subset('in', ['b', 'c']), set([1, 2]))
        self.assertEqual(index.subset('in', ['a', 'b', 'c']), set([0, 1, 2]))

    def test_index_update(self):
        index = self._basic_index() # 0=a, 1=b, 2=c
        
        index.update(0, 'd')

        self.assertEqual(index.index('d'), set([0]))
        self.assertEqual(index.index('a'), set([]))

        self.assertEqual(index.subset('gt', 'a'), set([0, 1, 2]))
        self.assertEqual(index.subset('gt', 'd'), set([]))
        self.assertEqual(index.subset('in', ['a', 'b']), set([1]))
        self.assertEqual(index.subset('in', ['c', 'd']), set([0, 2]))

    def test_bad_remove(self):
        index = self._basic_index() # 0=a, 1=b, 2=c

        self.assertRaises(CollectionIndexError, index.remove, 3)

    def test_multi_index(self):
        index = self._basic_index() # 0=a, 1=b, 2=c
        index.update(3, 'a')

        self.assertEqual(index.index('a'), set([0, 3]))
        self.assertEqual(index.index('b'), set([1]))
        self.assertEqual(index.subset('lt', 'b'), set([0, 3]))
        self.assertEqual(index.subset('in', ['a', 'b']), set([0, 1, 3]))

    def test_skip_index(self):
        index = self._basic_index() # 0=a, 1=b, 2=c
        index.update(5, 'd')

        self.assertEqual(index.index('d'), set([5]))

class UniqueIndexTest(unittest.TestCase): 
    def _basic_index(self):
        index = UniqueIndex('test')
        index.update(0, 'a')
        index.update(1, 'b')
        index.update(2, 'c')
        
        return index
    
    def test_create_index(self):
        #setup
        index = UniqueIndex('test')

        self.assertEqual(index.key, 'test')
        
    def test_good_index(self):
        index = self._basic_index()

        self.assertEqual(index.index('a'), 0)
        self.assertEqual(index.index('b'), 1)
        self.assertEqual(index.index('c'), 2)
        self.assertRaises(CollectionIndexError, index.index, 'd')

    def test_good_subset(self):
        index = self._basic_index()

        self.assertEqual(index.subset('eq', 'a'), set([0]))
        self.assertEqual(index.subset('eq', 'b'), set([1]))
        self.assertEqual(index.subset('eq', 'c'), set([2]))
        self.assertRaises(CollectionIndexError, index.subset, 'eq', 'd')

    def test_good_update(self):
        index = self._basic_index()

        index.update(0, 'a')
        
    def test_bad_update(self):
        index = self._basic_index()

        self.assertRaises(CollectionIndexError, index.update, 2, 'a')
        
    
