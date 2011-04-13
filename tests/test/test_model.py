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
    parent_model = PdbModel(key_name='parent_model')
    model = PdbModel(key_name='test_model',parent=parent_model,name=self.setup_name)
    int_model = PdbModel(name=self.setup_name_int,parent=parent_model)
    self.setup_key = pdb.put(model,_storage=['local','memcache','datastore'])
    self.parent_key = pdb.put(parent_model,_storage=['local','memcache','datastore'])
    self.setup_key_int = pdb.put(int_model,_storage=['local','memcache','datastore'])
    
  def tearDown(self):
    self.testbed.deactivate() 
    
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
    #Add this after doing GqlQuery tests
    pass
  
  def test_get_by_key_name(self):
    local_entity = PdbModel.get_by_key_name(self.setup_key.name(),
                                            parent = self.parent_key,
                                            _storage='local')
    memcache_entity = PdbModel.get_by_key_name(self.setup_key.name(),
                                               parent = self.parent_key,
                                               _storage='memcache')
    db_entity = PdbModel.get_by_key_name(self.setup_key.name(),
                                         parent = self.parent_key,
                                         _storage='datastore')

    self.assertEqual(local_entity.name,self.setup_name)
    self.assertEqual(memcache_entity.name,self.setup_name)
    self.assertEqual(db_entity.name,self.setup_name)
    
  def test_get_by_id(self):
    local_entity = PdbModel.get_by_id(self.setup_key_int.id(),
                                      parent = self.parent_key,
                                      _storage='local')
    memcache_entity = PdbModel.get_by_id(self.setup_key_int.id(),
                                         parent = self.parent_key,
                                         _storage='memcache')
    db_entity = PdbModel.get_by_id(self.setup_key_int.id(),
                                   parent = self.parent_key,
                                   _storage='datastore')

    self.assertEqual(local_entity.name,self.setup_name_int)
    self.assertEqual(memcache_entity.name,self.setup_name_int)
    self.assertEqual(db_entity.name,self.setup_name_int)
  
  def test_get_or_insert(self):
    #Existing entity
    local_entity = PdbModel.get_or_insert(self.setup_key.name(),
                                          parent = self.parent_key,
                                          name='Different name'
                                          ,_storage='local')
    memcache_entity = PdbModel.get_or_insert(self.setup_key.name(),
                                             parent = self.parent_key,
                                             name='Different name',
                                             _storage='memcache')
    db_entity = PdbModel.get_or_insert(self.setup_key.name(),
                                       parent = self.parent_key,
                                       name='Different name',
                                       _storage='datastore')

    self.assertEqual(local_entity.name,self.setup_name)
    self.assertEqual(memcache_entity.name,self.setup_name)
    self.assertEqual(db_entity.name,self.setup_name)
    
    #New entity
    key_name = 'new_entity'
    local_entity = PdbModel.get_or_insert(key_name,
                                          name='Different name'
                                          ,_storage='local')
    memcache_entity = PdbModel.get_or_insert(key_name,
                                             name='Different name',
                                             _storage='memcache')
    db_entity = PdbModel.get_or_insert(key_name,
                                       name='Different name',
                                       _storage='datastore')

    self.assertEqual(local_entity.name,'Different name')
    self.assertEqual(memcache_entity.name,'Different name')
    self.assertEqual(db_entity.name,'Different name')
  
  def test_cached_ref(self):
    class RefModel(pdb.Model):
      reference = db.ReferenceProperty(PdbModel)
      
    ref_model = RefModel(reference=self.setup_key)
    ref_model.put()
    
    local_entity = ref_model.cached_ref('reference',_storage='local')
    memcache_entity = ref_model.cached_ref('reference',_storage='memcache')
    db_entity = ref_model.cached_ref('reference',_storage='datastore')

    self.assertEqual(local_entity.name,self.setup_name)
    self.assertEqual(memcache_entity.name,self.setup_name)
    self.assertEqual(db_entity.name,self.setup_name)
  
  def test_cached_set(self):
    class RefModel(pdb.Model):
      reference = db.ReferenceProperty(PdbModel)
      
    models = []
    for i in range(100):
      models.append(RefModel(reference=self.setup_key))
      
    pdb.put(models)
    pdb_model = pdb.get(self.setup_key)
    
    #First call creates memcache index
    refs = pdb_model.cached_set('refmodel_set')
    self.assertEqual(len(refs),len(models))