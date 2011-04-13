import unittest
import logging
from google.appengine.ext import db
from google.appengine.api import memcache
from google.appengine.ext import testbed
from PerformanceEngine import pdb
from models import PdbModel

class ModelTest(unittest.TestCase):
  
  def setUp(self):
    self.testbed = testbed.Testbed()
    self.testbed.activate()
    self.testbed.init_datastore_v3_stub()
    self.testbed.init_memcache_stub()
    self.setup_name = 'test'
    self.setup_name_int = 'int_model'
    model = PdbModel(key_name='test_model',name=self.setup_name)
    int_model = PdbModel(name=self.setup_name_int)
    self.setup_key = pdb.put(model,_storage=['local','memcache','datastore'])
    self.setup_key_int = pdb.put(int_model,_storage=['local','memcache','datastore'])
    
  def test_get(self):  
    local_entity = PdbModel.get(self.setup_key,_storage='local')
    memcache_entity = PdbModel.get(self.setup_key,_storage='memcache')
    db_entity = PdbModel.get(self.setup_key,_storage='datastore')
    
    self.assertEqual(local_entity.name,self.setup_name)
    self.assertEqual(memcache_entity.name,self.setup_name)
    self.assertEqual(db_entity.name,self.setup_name)
  
  
  def test_put(self):
    model = PdbModel(name='put_test')
    key = model.put(_storage=['local','memcache','datastore'])
    
    local_entity = PdbModel.get(key,_storage='local')
    memcache_entity = PdbModel.get(key,_storage='memcache')
    db_entity = PdbModel.get(key,_storage='datastore')
    
    self.assertEqual(local_entity.name,'put_test')
    self.assertEqual(memcache_entity.name,'put_test')
    self.assertEqual(db_entity.name,'put_test')                
                 
  
  def test_delete(self):
    model = PdbModel.get(self.setup_key,_storage=['local','memcache','datastore'])
    model.delete(_storage=['local','memcache','datastore'])
    
    local_entity = PdbModel.get(self.setup_key,_storage='local')
    memcache_entity = PdbModel.get(self.setup_key,_storage='memcache')
    db_entity = PdbModel.get(self.setup_key,_storage='datastore')
    
    self.assertEqual(local_entity , None)
    self.assertEqual(memcache_entity , None)
    self.assertEqual(db_entity , None)
  
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
