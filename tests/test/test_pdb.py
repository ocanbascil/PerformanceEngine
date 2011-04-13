import unittest
import logging
from google.appengine.ext import db
from google.appengine.api import memcache
from google.appengine.ext import testbed
from PerformanceEngine import pdb,_serialize,_deserialize,cachepy
from models import TestModel


class GetTest(unittest.TestCase):

  def setUp(self):
    self.testbed = testbed.Testbed()
    self.testbed.activate()
    self.testbed.init_datastore_v3_stub()
    self.testbed.init_memcache_stub()
    model = TestModel(key_name='test_model',name='test')
    self.setup_key = db.put(model)
    cache_key = str(self.setup_key)
    cachepy.set(cache_key, model)
    memcache.set(cache_key, _serialize(model))
    
  def tearDown(self):
    self.testbed.deactivate() 
  
  def test_get_db(self):
    entity = pdb.get(self.setup_key,_storage='datastore')
    self.assertEqual('test', entity.name)
  
  def test_get_memcache(self):
    entity = pdb.get(self.setup_key,_storage=['memcache'])
    self.assertEqual('test', entity.name)
      
  def test_get_local(self):
    entity = pdb.get(self.setup_key,_storage=['local'])
    self.assertEqual('test', entity.name)
    
  def test_cascaded_cache_refresh(self):
    e1 = TestModel()
    k1 = db.put(e1)
    #Memcache refresh from datastore
    pdb.get(k1,_storage =['memcache','datastore'] )
    e2 = pdb.get(k1,_storage ='memcache')
    self.assertEqual(e1.key(),e2.key())
    
    #Local refresh from datastore
    pdb.get(k1,_storage =['local','datastore'])
    e2 = pdb.get(k1,_storage='local')
    self.assertEqual(e1.key(),e2.key())
    
    #Local refresh from memcache
    e3 = TestModel(key_name='memcache_model')
    k3 = pdb.put(e3,_storage='memcache')
    pdb.get(k3, _storage=['local','memcache'])
    e4 = pdb.get(k3,_storage='local')
    self.assertEqual(e3.key(),e4.key())
    
  def test_result_type(self):
    single_result = pdb.get(self.setup_key)
    self.assertTrue(isinstance(single_result, db.Model))
    
    dict_result = pdb.get(self.setup_key,_result_type='dict')
    self.assertTrue(isinstance(dict_result, dict))
    self.assertEqual(dict_result.keys()[0],str(self.setup_key))
    self.assertEqual(dict_result.values()[0].name,'test')
    
    name_dict_result = pdb.get(self.setup_key,_result_type='name_dict')
    self.assertTrue(isinstance(name_dict_result, dict))
    self.assertEqual(name_dict_result.keys()[0],self.setup_key.name())
    self.assertEqual(name_dict_result.values()[0].name,'test')
    
    #Check for integer based key
    e1 = TestModel(name='integer_test')
    k1 = pdb.put(e1)
    name_dict_result = pdb.get(k1,_result_type='name_dict')    
    self.assertTrue(isinstance(name_dict_result, dict))
    self.assertEqual(name_dict_result.keys()[0],str(k1.id()))
    self.assertEqual(name_dict_result.values()[0].name,'integer_test')    
        
class PutTest(unittest.TestCase):
  
  def setUp(self):
    self.testbed = testbed.Testbed()
    self.testbed.activate()
    self.testbed.init_datastore_v3_stub()
    self.testbed.init_memcache_stub()
    model = TestModel(key_name='test_key_name')
    self.setup_key = pdb.put(model,_storage=['local','memcache','datastore'] )
    self.cache_key = str(self.setup_key)
 
  def test_put_db(self):
    model = TestModel(key_name='test_key_name',name='test')
    key = pdb.put(model,_storage='datastore')
    self.assertEqual('test', db.get(key).name)
    
  def test_put_memcache(self):  
    model = TestModel(key_name='test_key_name',name='test')
    key = pdb.put(model,_storage='memcache')
    entity = _deserialize(memcache.get(str(key)))
    self.assertEqual('test', entity.name)
    
  def test_put_local(self):
    model = TestModel(key_name='test_key_name',name='test')
    key = pdb.put(model,_storage='local')
    entity = cachepy.get(str(key))
    self.assertEqual('test', entity.name)
    
class DeleteTest(unittest.TestCase):
  
  def setUp(self):
    self.testbed = testbed.Testbed()
    self.testbed.activate()
    self.testbed.init_datastore_v3_stub()
    self.testbed.init_memcache_stub()
    model = TestModel(key_name='test_key_name')
    self.setup_key = pdb.put(model,_storage=['local','memcache','datastore'] )
    self.cache_key = str(self.setup_key)

  def test_delete_db(self):
    pdb.delete(self.setup_key,_storage='datastore')
    entity = db.get(self.setup_key)
    self.assertEqual(entity , None)
    
  def test_delete_memcache(self):  
    pdb.delete(self.setup_key,_storage='memcache')
    entity = _deserialize(memcache.get(self.cache_key))
    self.assertEqual(entity , None)
    
  def test_delete_local(self):
    pdb.delete(self.setup_key,_storage='local')
    entity = cachepy.get(self.cache_key)
    self.assertEqual(entity , None)
