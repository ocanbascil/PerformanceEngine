import unittest
import logging
from google.appengine.ext import db
from google.appengine.api import memcache
from google.appengine.ext import testbed
from PerformanceEngine import pdb,cachepy,_deserialize
from models import PdbModel


class QueryTest(unittest.TestCase):
  def setUp(self):
    self.testbed = testbed.Testbed()
    self.testbed.activate()
    self.testbed.init_datastore_v3_stub()
    self.testbed.init_memcache_stub()
    self.query = pdb.GqlQuery('SELECT * FROM PdbModel')
    models = []
    for i in range(100):
      models.append(PdbModel(count=i))
      
    pdb.put(models)

  def tearDown(self):
    self.testbed.deactivate() 
      
  def test_bind(self):
    positional_query = pdb.GqlQuery('SELECT * FROM PdbModel WHERE count >:1 AND name=:2')
    keyword_query = pdb.GqlQuery('SELECT * FROM PdbModel WHERE count > :count AND name=:name')
    
    positional_query.bind(5,'test_name')
    keyword_query.bind(count=5,name='test_name')
  
  def test_fetch(self):
    db_models = self.query.fetch(100,_cache=['local','memcache'])
    cache_key = self.query.key_name
    memcache_models = _deserialize(memcache.get(cache_key))
    local_models = cachepy.get(cache_key)
    
    self.assertEqual(len(db_models),100)
    self.assertEqual(len(memcache_models),100)
    self.assertEqual(len(local_models),100)
    self.assertEqual(db_models[0].key(),memcache_models[0].key())
    self.assertEqual(db_models[0].key(),local_models[0].key())      
    
    db_models = self.query.fetch(100,offset=50,_cache=['local','memcache'])
    cache_key = self.query.key_name
    memcache_models = _deserialize(memcache.get(cache_key))
    local_models = cachepy.get(cache_key)
    
    self.assertEqual(len(db_models),50)
    self.assertEqual(len(memcache_models),50)
    self.assertEqual(len(local_models),50)
    self.assertEqual(db_models[0].key(),memcache_models[0].key())
    self.assertEqual(db_models[0].key(),local_models[0].key())  
  
  def test_get(self):
    db_entity = self.query.get(_cache=['local','memcache'])
    cache_key = self.query.key_name
    memcache_entity = _deserialize(memcache.get(cache_key))[0]
    local_entity = cachepy.get(cache_key)[0]
 
    self.assertEqual(db_entity.count,0)
    self.assertEqual(db_entity.key(),memcache_entity.key())
    self.assertEqual(memcache_entity.key(),local_entity.key())      
    
    db_entity = self.query.get(offset=50,_cache=['local','memcache'])
    cache_key = self.query.key_name
    memcache_entity = _deserialize(memcache.get(cache_key))[0]
    local_entity = cachepy.get(cache_key)[0]
    
    self.assertEqual(db_entity.count,50)
    self.assertEqual(db_entity.key(),memcache_entity.key())
    self.assertEqual(memcache_entity.key(),local_entity.key())    
  
  def test_count(self):
    self.assertEqual(self.query.count(),100)
  
  def test_cursor(self):
    results = self.query.fetch(10)
    cursor = self.query.cursor()
    self.assertTrue(isinstance(cursor, str))
  
  def test_with_cursor(self):
    results = self.query.fetch(10)
    cursor = self.query.cursor()
    
    self.assertEqual(results[0].count,0)
    
    self.query.with_cursor(cursor)
    results = self.query.fetch(10)
    end_cursor = self.query.cursor()
    
    self.assertEqual(results[0].count,10)
    
    self.query.with_cursor(cursor, end_cursor)
    results = self.query.fetch(1000)
    
    self.assertEqual(len(results),10)
    self.assertEqual(results[0].count,10)
    self.assertEqual(results[9].count,19)
    
    self.query.with_cursor(None)
    results = self.query.fetch(10)
    self.assertEqual(len(results),10)
    self.assertEqual(results[0].count,0)
    