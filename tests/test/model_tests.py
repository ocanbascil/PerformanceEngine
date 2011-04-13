import unittest
import logging
from google.appengine.ext import db
from google.appengine.api import memcache
from google.appengine.ext import testbed
from PerformanceEngine import pdb

class TestModel(pdb.Model):
  name = db.StringProperty()

class ModelTest(unittest.TestCase):
  
  def setUp(self):
    self.testbed = testbed.Testbed()
    self.testbed.activate()
    self.testbed.init_datastore_v3_stub()
    self.testbed.init_memcache_stub()
    self.setup_name = 'test'
    self.setup_name_int = 'int_model'
    model = TestModel(key_name='test_model',name=self.setup_name)
    int_model = TestModel(name=self.setup_name_int)
    self.setup_key = pdb.put(model,_storage=['local','memcache','datastore'])
    self.setup_key_int = pdb.put(int_model,_storage=['local','memcache','datastore'])
    

    
  def test_get(self):  
    local_entity = TestModel.get(self.setup_key,_storage='local')
    memcache_entity = TestModel.get(self.setup_key,_storage='memcache')
    db_entity = TestModel.get(self.setup_key,_storage='datastore')
    
    self.assertEqual(local_entity.name,self.setup_name)
    self.assertEqual(memcache_entity.name,self.setup_name)
    self.assertEqual(db_entity.name,self.setup_name)
  
  
  def test_put(self):
    pass
  
  def test_delete(self):
    pass
  
  def test_gql(self):
    pass
  
  def test_get_by_key_name(self):
    pass
  
  def test_get_by_id(self):
    pass
  
  def test_get_or_insert(self):
    pass
  
  def test_cached_ref(self):
    pass
  
  def test_cached_set(self):
    pass
