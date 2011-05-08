# Copyright (C) 2011 O. Can Bascil <ocanbascil at gmail com>
"""
PerformanceEngine
v1.0
https://github.com/ocanbascil/Performance-AppEngine
==============================
    PerformanceEngine is a simple wrapper module that enables layered 
    data model storage and cached queries in Google Application Engine. 
    Its main goal is to increase both application and developer performance.
    
    It can store/retrieve models using local cache, memcache or datastore.
    
    You can also retrieve results in different formats (list,key-model dict,
    key_name-model dict)
 
Requirements
------------
cachepy => http://appengine-cookbook.appspot.com/recipe/cachepy-faster-than-memcache-and-unlimited-quota/

License
-------
This program is release under the BSD License. You can find the full text of
the license in the LICENSE file.
"""
from google.appengine.api import memcache
from google.appengine.ext import db
from google.appengine.api import datastore
from google.appengine.ext import deferred
from google.appengine.datastore import entity_pb
from google.appengine.runtime import apiproxy_errors

import cachepy
import logging

from datetime import datetime,date

'''Constants for storage levels'''
DATASTORE = 'datastore'
MEMCACHE = 'memcache'
LOCAL = 'local'
ALL_LEVELS = [DATASTORE,MEMCACHE,LOCAL]
ALL_CACHE = [MEMCACHE,LOCAL]

'''Constants for result types'''
LIST = 'list'
DICT = 'dict'
NAME_DICT = 'name_dict'

LOCAL_EXPIRATION = 300
MEMCACHE_EXPIRATION = 0
QUERY_EXPIRATION = 300

none_filter  = lambda dict : [k for k,v in dict.iteritems() if v is None]

_dict_multi_get = lambda keys,dict : [v for (k,v) in dict.iteritems() if k in dict if v is not None]

def _validate_storage(storage_list):
  for storage in storage_list:
    if storage not in ALL_LEVELS:
      raise StorageLayerError(storage)
    
def _validate_cache(cache_list):
  for cache in cache_list:
    if cache not in ALL_CACHE:
      raise CacheLayerError(cache)

def _key_str(param):
  '''Utility function that extracts a string key from a model or key instance'''
  try:
    return str(db._coerce_to_key(param))
  except db.BadArgumentError:
    raise KeyParameterError(param)
  
def _id_or_name(_key_str):
  key = db.Key(_key_str)
  return key.name() or str(key.id())

def _to_list(param): 
    if not isinstance(param,list):
        result = []
        result.append(param)
    else:
        result = list(param)
    return result

def _to_dict(models):
  '''Utility method to create identifier:model dictionary'''
  result = {}
  for model in models:
    result[_key_str(model)] = model
  return result

def _serialize(models):
  '''Improve memcache performance converting to protobuf'''
  if models is None:
    return None
  elif isinstance(models, db.Model):
    # Just one instance
    return db.model_to_protobuf(models).Encode()
  else:
    # A list
    return [db.model_to_protobuf(x).Encode() for x in models]

def _deserialize(data):
  '''Improve memcache performance by converting from protobuf'''
  if data is None:
    return None
  elif isinstance(data, str):
    # Just one instance
    return db.model_from_protobuf(entity_pb.EntityProto(data))
  else:
    return [db.model_from_protobuf(entity_pb.EntityProto(x)) for x in data]

def _cachepy_get(keys):
  '''Get items with given keys from local cache'''
  result = {}
  for key in keys:
    result[key] = cachepy.get(key)
  return result

def _cachepy_put(models,time = 0):
  '''Put given models to local cache in serialized form
   with expiration in seconds
  
  Args:
    models: List of models to be saved to local cache
    time: Expiration time in seconds for each model instance
  
  Returns:
    List of  of db.Keys of the models that were put
  '''
  to_put = _to_dict(models)
  if time == 0: #cachepy uses None as unlimited caching flag
    time = None
  
  for key, model in to_put.iteritems():
    cachepy.set(key,model,time)
  return [model.key() for model in models]

def _cachepy_delete(keys):
  '''Delete models with given keys from local cache'''
  for key in keys: 
      cachepy.delete(key)

def _memcache_get(keys):
  '''Get items with given keys from memcache
    If no model is found for given key, value for that key
    in result is set to None
  '''
  cache_results = memcache.get_multi(keys)
  result = {}
  for key in keys:
    try:
      result[key] = _deserialize(cache_results[key])
    except KeyError:
      result[key] = None
  return result
    
def _memcache_put(models,time = 0):
  '''Put given models to memcache in serialized form
   with expiration in seconds
     
  Returns:
    List of  db.Keys of the models that were put
  '''         
  to_put = _to_dict(models)
        
  for key,model in to_put.iteritems():
      to_put[key] = _serialize(model)
          
  memcache.set_multi(to_put,time)
  return [model.key() for model in models]

def _memcache_delete(keys):
  '''Delete models with given keys from memcache'''
  memcache.delete_multi(keys)
  
def _put(models,countdown=0):
  batch_size = 50
  to_put = []
  keys = []
  try:
    last_index = 0
    for i,model in enumerate(models):
      to_put.append(model)
      last_index = i
      if (i+1) % batch_size == 0:
        keys.extend(db.put(to_put))
        to_put = []
    keys.extend(db.put(to_put))
    return keys
    
  except apiproxy_errors.DeadlineExceededError:
    keys.extend(db.put(to_put))
    deferred.defer(_put,models[last_index+1:],_countdown=10)
    return keys
  
  except apiproxy_errors.CapabilityDisabledError:
    if not countdown:
      countdown = 30
    else:
      countdown *= 2
    deferred.defer(_put,models,countdown,_countdown=countdown)
      
  
class pdb(object):
  '''Wrapper class for google.appengine.ext.db with seamless cache support'''
  
  @classmethod
  def get(cls,keys,_storage = None,
          _local_expiration = LOCAL_EXPIRATION,
          _memcache_expiration = MEMCACHE_EXPIRATION,
          _result_type=LIST,
          **kwds):
    """Fetch the specific Model instance with the given keys from 
    given storage layers in given format. 
    
    WARNING: If you try to get different model kinds with the same key
    names and use NAME_DICT as result type, you'll lose data as
    models with same key_names will overwrite each other
  
    Args:
      _storage: string or array of strings for target storage layers.
      _local_expiration: Time for local cache expiration in seconds
                              'local' is not in _storage parameters.
      _memcache_expiration: Time for memcache expiration in seconds
                              'memcache' is not in _storage parameters.
      _result_type: format of the result 
      
      Inherited:
        keys: Key within datastore entity collection to find; or string key;
          or list of Keys or string keys.
        config: datastore_rpc.Configuration to use for this request.
      
    Returns:
      if _result_type = LIST:
        If a single key was given: a Model instance associated with key
        for if it exists in the datastore, otherwise None; if a list of
        keys was given: a list whose items are either a Model instance or
        None.
      if _result_type = DICT:
        A key-model dictionary
      if _result_type = NAME_DICT
        A key_name-model dictionary / str(id)-model dictionary
        
    Raises:
      ResultTypeError: if an invalid result type is supplied
      StorageLayerError: If an invalid storage parameter is given  
      KeyParameterError: If something other than db.Key or string repr.
        of db.Key is given
    """
    if _storage is None:
      _storage = [MEMCACHE,DATASTORE]
    else:
      _storage = _to_list(_storage)
      _validate_storage(_storage)
    
    keys = map(_key_str,_to_list(keys))
    old_keys = keys
    local_not_found = []
    memcache_not_found = []
    local_flag = True if LOCAL in _storage else False
    memcache_flag = True if MEMCACHE in _storage else False
    models = {}
    
    if local_flag:
      models = dict(models,**_cachepy_get(keys))
      keys = none_filter(models)
      local_not_found = keys
          
    if memcache_flag and len(keys):
      models = dict(models,**_memcache_get(keys))
      keys = none_filter(models)
      memcache_not_found = keys
    
    if DATASTORE in _storage and len(keys):
      db_results = [model for model in db.get(keys) if model is not None]
      if len(db_results):
        models  = dict(models,**_to_dict(db_results))
        
    if local_flag:
      targets = _dict_multi_get(local_not_found, models)
      if len(targets):
        pdb.put(targets,_storage = LOCAL,
                _local_expiration = _local_expiration,**kwds)  
    
    if memcache_flag:
      targets = _dict_multi_get(memcache_not_found,models)
      if len(targets):  
        pdb.put(targets,_storage = MEMCACHE,
                _memcache_expiration = _memcache_expiration,**kwds)
        
    result = []    
    if _result_type == LIST:
      #Restore the order of entities   
      for key in old_keys:
        try:
          result.append(models[key])
        except KeyError:
          result.append(None)   
      #Normalized result
      if len(result) > 1:
        return result
      return result[0]
    elif _result_type == DICT:
      return models
    elif _result_type == NAME_DICT:
      result = {}
      for k,v in models.iteritems():
        result[_id_or_name(k)] = v
      return result
    else:
      raise ResultTypeError(_result_type)
        

  @classmethod
  def put(cls,models,_storage = None,
                      _local_expiration = LOCAL_EXPIRATION,
                      _memcache_expiration = MEMCACHE_EXPIRATION,
                       **kwds):
    '''Saves models into given storage layers and returns their keys
    
    If the models are written for the first time and they have no key names ,
    They are first written into datastore and then saved to other storage layers
    using the keys returned by datastore put() operation.
    
    Args:

      _storage: string or array of strings for target storage layers  
      _local_expiration: Time in seconds for local cache expiration for models
      _memcache_expiration: Time in seconds for memcache expiration for models
    
      Inherited:
        models: Model instance or list of Model instances.
        config: datastore_rpc.Configuration to use for this request.
    
    Returns:
      A Key or a list of Keys (corresponding to the argument's plurality).
    
    Raises:
      IdentifierNotFoundError if models has no key names 
      and are being written into cache storage only.
    
      Inherited:
        TransactionFailedError if the data could not be committed.
    '''

    keys = [] 
    models = _to_list(models)   
    models = [model for model in models if model is not None]
    if _storage is None:
      _storage = [MEMCACHE,DATASTORE]
    else:
      _storage = _to_list(_storage)
      _validate_storage(_storage)
    
    try: 
      _to_dict(models)
    except db.NotSavedError:
      if DATASTORE in _storage:
        keys = _put(models)
        models = db.get(keys)
        _storage.remove(DATASTORE)
        if len(_storage):
          return pdb.put(models,_storage,**kwds)
      else: 
        raise IdentifierNotFoundError() 
    
    if DATASTORE in _storage:
      keys = _put(models)
      
    if LOCAL in _storage:
      keys = _cachepy_put(models, _local_expiration)

    if MEMCACHE in _storage:
      keys = _memcache_put(models,_memcache_expiration)
      
    if len(keys) > 1:
      return keys
    elif len(keys):
      return keys[0]
    else:
      return None


  @classmethod
  def delete(cls,keys,_storage = None):
    """Delete one or more Model instances from given storage layers
  
    Args:
      _storage: string or array of strings for target storage layers
      
      Inherited:
        models: Model instance, key, key string or iterable thereof.
        config: datastore_rpc.Configuration to use for this request.
    """
    keys = map(_key_str, _to_list(keys))
    if _storage is None:
      _storage = ALL_LEVELS
    else:
      _storage = _to_list(_storage)
      _validate_storage(_storage)
    
    if DATASTORE in _storage:
      db.delete(keys)
      
    if LOCAL in _storage:
      _cachepy_delete(keys)

    if MEMCACHE in _storage:
      _memcache_delete(keys)
  
  
  class Model(db.Model):
    '''Wrapper class for db.Model
    Adds cached storage support to common functions'''
    
    _default_delimiter = '|'
    
    def put(self,**kwds):
      """Writes this model instance to the given storage layers.
  
      Args:
         _storage: string or array of strings for target storage layers  
        _local_expiration: Time in seconds for local cache expiration for models
        _memcache_expiration: Time in seconds for memcache expiration for models       
        
        Inherited:
          config: datastore_rpc.Configuration to use for this request.
  
      Returns:
        The key of the instance (either the existing key or a new key).
  
      Raises:
        IdentifierNotFoundError if model has no key_name and being
          written into cache storage only.       
        
        Inherited:
          TransactionFailedError if the data could not be committed.
      """
      return pdb.put(self, **kwds)
    
    @classmethod
    def get(cls,keys,**kwds):
      '''Fetch a specific Model type instance from given storage 
      layers, using keys. 
      Args:
        See pdb.get
        
        Inherited:
          keys: Key within datastore entity collection to find; or string key;
            or list of Keys or string keys.
          config: datastore_rpc.Configuration to use for this request.
  
      Returns:
        See pdb.get
        
      Raises:
        KindError if any of the retrieved objects are not instances of the
          type associated with call to 'get'.
      '''
      models = pdb.get(keys,**kwds)
      
      #Class kind check
      temp = models
      if isinstance(temp,dict):
        temp = dict.values()
      elif isinstance(temp, db.Model):
        temp = [temp]
        
      if temp is None:
        return None
    
      for instance in temp:
        if not isinstance(instance, cls) and instance is not None:
          raise db.KindError('Kind %r is not a subclass of kind %r' %
                          (instance, cls)) 
      return models
    
    def delete(self,_storage = ALL_LEVELS):
      """Delete current instance from given storage layers
    
      Args:
        _storage: string or array of strings for target storage layers
      """
      pdb.delete(self.key(),_storage)
    
    @classmethod
    def get_by_key_name(cls,key_names, parent=None,**kwds):
      """Get instance of Model class by its key's name from the given storage layers.
      Args:
          See pdb.get
        
        Inherited:
          key_names: A single key-name or a list of key-names.
          parent: Parent of instances to get.  Can be a model or key.
      """
      try:
        parent = db._coerce_to_key(parent)
      except db.BadKeyError, e:
        raise db.BadArgumentError(str(e))
      
      key_names = _to_list(key_names)
      key_strings = [_key_str(db.Key.from_path(cls.kind(), name, parent=parent))
        for name in key_names]

      return pdb.get(key_strings,**kwds)
    
    @classmethod
    def get_by_id(cls, ids, parent=None,**kwds):
      """Get instance of Model class by id from the given storage layers.
      Args:
         See pdb.get
         
        Inherited:
          ids: A single id or a list of ids.
          parent: Parent of instances to get.  Can be a model or key.
      """
      try:
        parent = db._coerce_to_key(parent)
      except db.BadKeyError, e:
        raise db.BadArgumentError(str(e))
      
      ids = _to_list(ids)
      key_strings = [_key_str(datastore.Key.from_path(cls.kind(), id, parent=parent))
        for id in ids]
      
      return pdb.get(key_strings,**kwds)
    
    @classmethod
    def get_or_insert(cls,key_name,parent=None,**kwds):
      '''Retrieve or create an instance of Model class using the given storage layers.
      
      If entity is found, it is returned and also cache layers are refreshed if the result
      isn't found in them. 
      
      If entity is not found, a new one is created an written into given storage layers.
      
      Args:
        See pdb.get
        
        Inherited:
          key_name: Key name to retrieve or create.
          **kwds: Keyword arguments to pass to the constructor of the model class
            if an instance for the specified key name does not already exist. If
            an instance with the supplied key_name and parent already exists, the
            rest of these arguments will be discarded.

      Returns:
        Existing instance of Model class with the specified key_name and parent
        or a new one that has just been created.
  
      Raises:
        TransactionFailedError if the specified Model instance could not be
        retrieved or created transactionally (due to high contention, etc).
      '''
      def txn():
        entity = cls(key_name=key_name, **kwds)
        entity.put(**kwds)
        return entity
        
      try:
        kwds.pop('_result_type') #Use default result for pdb.get
      except KeyError:
        pass
      entity = cls.get_by_key_name(key_name,parent=parent,**kwds)
      if entity is None:
        return db.run_in_transaction(txn)
      else:
        return entity
      
    def cached_ref(self,reference_name,**kwds):
      '''This function is a wrapper around db.ReferenceProperty
      When a reference name is given this function tries to retrieve 
      the model from given storage layers using the pdb.get function.
      '''
      try:
        property = self.properties()[reference_name]
      except KeyError:
        raise ReferenceError(reference_name, ReferenceError.REFERENCE_NAME_ERROR)
      if not isinstance(property, db.ReferenceProperty):
        raise ReferenceError(property.__class__.__name__,ReferenceError.TYPE_ERROR)

      try:
        kwds.pop('_result_type') #Use default result for pdb.get
      except KeyError:
        pass
      return pdb.get(property.get_value_for_datastore(self),**kwds)
      
    def cached_set(self,collection_name,index_expiration=300,**kwds):
      '''This function is a wrapper around back-reference functionality of 
      db.ReferenceProperty,allowing cached retrieval of models that reference 
      this entity.
      
      If a _ReferenceCacheIndex is found, a default pdb.get function is called
      Otherwise  the query is run and models are returned
      
      WARNING: Instead of a query instance, this function fetches and returns 
      the actual models, so it is advised not to use it for models that have 
      a high number of back-references.
      
      Basic Usage:
        class MainModel(pdb.Model):
          add_date = db.DateProperty(auto_now = True) 
        
        class RefModel(db.Model):
          ref = db.ReferenceProperty(reference_class=MainModel,
                                              collection_name = 'ref_set')
                                              
        model = MainModel.all().get()
        model.ref_set #models that reference this MainModel entity
        model.cached_set('ref_set') #Cached back-references
      
      Args:
        collection_name: Name of the back reference collection
        index_expiration: Memcache expiration time for _ReferenceCacheIndex
        entity that'll be created for this reference set if there isn't any.
        
        See pdb.get for additional parameters
        
      Returns:
        A list of models that back reference this entity
      
      Raises:
        ReferenceError: If an invalid collection name is supplied
      '''
      property = getattr(self, collection_name)
      if not isinstance(property, db.Query):
        raise ReferenceError(collection_name,ReferenceError.COLLECTION_NAME_ERROR)
      
      klass = self.__class__
      key_name = str(self.key())+klass._default_delimiter+collection_name
      query_cache = _ReferenceCacheIndex.get_by_key_name(key_name,
                                                         _storage=MEMCACHE)
      if query_cache:
        try:
          kwds.pop('_result_type')  #Use default result for pdb.get
        except KeyError:
          pass
        keys = query_cache.ref_keys
        result =  pdb.get(keys,**kwds)
      else: 
        result = [model for model in getattr(self,collection_name)]
        _ReferenceCacheIndex.create(self,collection_name,result,index_expiration)
      return result
    
    @classmethod
    def gql(cls, query_string, *args, **kwds):
      """Returns a cached query using GQL query string.
      See pdb.GqlQuery for detail cache functionality usage.

      Args:
        query_string: properly formatted GQL query string with the
          'SELECT * FROM <Model>' part omitted
        *args: rest of the positional arguments used to bind numeric references
          in the query.
        **kwds: dictionary-based arguments (for named parameters).
      """
      return pdb.GqlQuery('SELECT * FROM %s %s' % (cls.kind(), query_string),
                      *args, **kwds)       
    
    def log_properties(self,console=False):
      '''Log properties of an entity'''
      result = 'Logging properties for %s with identifier: %s =>' \
      %(self.__class__.kind(),_id_or_name(str(self.key())))
      for k,v in self.properties().iteritems():
        prop = '%s : %s' %(k,v.get_value_for_datastore(self))
        result += '('+prop+'),'
      if console:
        print result
      else:
        logging.info(result)
        
  class GqlQuery(object):
    '''This class is a wrapper that adds cache support to GQL queries
      See Google App Engine docs for basic GqlQuery Usage
      
      Usage:
        Create a pdb.GqlQuery instance and bind variables as you like.
        When you do a fetch, indicate if you want cache support with
        this query by supplying cache and expiration parameters. 
        
        Default usage with no additional parameters has the same 
        functionality of a db.GqlQuery.
        
        If no cache match is found and at least one cache layer is supplied,
        after the query is run on datastore the result will be stored in 
        cache for future calls.
        
        Do not use __limit__ or __fetch__ for names while binding 
        parameters to a query.
        
      Example:
        query = pdb.GqlQuery('SELECT * FROM SomeModel WHERE count =:1',42)
        
        #Results are fetched from datastore and saved to memcache for 2 minutes
        results = query.fetch(15,_cache=['memcache'],_memcache_expiration=120)

        #This time fetch results from memcache and also refresh the local cache 
        #Similar to cascaded cache refresh in pdb.get
        results = query.fetch(15,_cache=['local','memcache'],
                                      _memcache_expiration=120,
                                      _local_expiration=120)
                                      
        WARNING: Creating cache keys for query results uses:
        1 - hashed value of query string
        2 - binded variables
        3- fetch and offset parameters. 
        
        This results in logically equivalent queries with different syntaxes 
        having different cache keys.
        
        Example 1: Equivalent queries with different query strings
        
          #Keyname root: GQL_-1132007280
          query1 = pdb.GqlQuery('SELECT * FROM SomeModel WHERE count =:1',42)
          
          #Keyname root: GQL_-143876660
          query2 = pdb.GqlQuery('SELECT * FROM SomeModel WHERE count =:count', count = 42)
          
          #Keyname root: GQL_-1145123476
          query3 = pdb.GqlQuery('select * from SomeModel where count =:count', count = 42)
          
        Example 2: Equal number of results with different fetch limits
          #Let's say we have only 10 occurences of a model in our database, for given parameters.
          #Following queries both return same result but have different cache keys
          
          #Query cache key: GQL_-1138707777|count:42|date:2011-04-05
          query = pdb.GqlQuery('SELECT * FROM SomeModel WHERE count =:count AND date=:date')
          query.bind(count=42,date=datetime.date.today())
          
          #Fetch cache key: GQL_-1138707777|count:42|date:2011-04-05|__limit__:20
          result1 = query.fetch(20,_cache='memcache')
          
          #Fetch cache key: GQL_-1138707777|count:42|date:2011-04-05|__limit__:100
          result2= query.fetch(100,_cache='memcache')
    '''
    delim  = '|'
    limit_key = '__limit__'
    offset_key = '__offset__'
    
    def __init__(self,query_string,*args,**kwds):
      self.key_name = 'GQL_'+str(hash(query_string))
      self.query = db.GqlQuery(query_string,*args,**kwds)
      if args or kwds:
        self.bind(*args,**kwds)
        
    def __iter__(self):
      '''Iterator for query instance'''
      return self.query.run()
            
    def _concat_keyname(self,param):
      '''Utility function for creating cache key
      adds param to the self.key_name with default delimiter'''
      klass = self.__class__
      self.key_name += klass.delim+param
      
    def _clear_keyname(self,key=None):
      '''Utility function for clearing cache key
      if key is None, it removes all suffixes and leaves cache key root
      otherwise it searches for given suffix and right trims the key at that point'''
      klass = self.__class__
      if key is not None:
        key_index = self.key_name.find(key)
        if key_index > 0:
          delim_index = key_index-1
        else:
          delim_index = None
      else:
        delim_index = self.key_name.find(klass.delim)

      if delim_index > 0:
        self.key_name = self.key_name[:delim_index]
        
    def _create_suffix(self,*args,**kwds):
      '''When args and kwds are binded to the query,
      this function creates a cache key using positional
      and key-value parameters
      
      positional values are concatenated by order
      key-value pairs are sorted by alphabetical order'''
      
      for item in args:
        self._concat_keyname(self._repr_param(item))
             
      sorted_keys = sorted(kwds.keys())
      for key in sorted_keys:
        self._concat_keyname(key+':'+self._repr_param(kwds[key]))
    
    def _repr_param(self,param):
      '''Converts query parameters to string representations'''
      if isinstance(param, db.Model):
        return str(param.key())
      else:
        return str(param)
      
    def bind(self,*args,**kwds):
      '''Binds arguments to the query and creates cache key'''
      self._clear_keyname()
      self._create_suffix(*args,**kwds)
      self.query.bind(*args,**kwds)
      
    def cursor(self):
      '''Returns the query cursor after a datastore query operation'''
      return self.query.cursor()
    
    def with_cursor(self,start_cursor, end_cursor=None):
      '''Runs the query on datastore using start and end cursors'''
      return self.query.with_cursor(start_cursor, end_cursor)
      
    def count(self,limit=1000):
      '''Return the number of entities for this query'''
      return self.query.count(limit)
      
    def get(self,**kwds):
      '''Return first or offset+1 nth element in query result'''
      try:
        return self.fetch(1,**kwds)[0]
      except IndexError:
        return None
      
    def fetch(self,limit,offset=0,
              _cache=None,
              _local_expiration = QUERY_EXPIRATION,
              _memcache_expiration = QUERY_EXPIRATION):
      '''By default this method runs the query on datastore.
      
      If additonal parameters are supplied, it tries to retrieve query
      results for current parameters and fetch & offset limits.
      
      It also does a cascaded cache refresh if no match for 
      current arguments are found in given cache layers.
      
      Arguments:
        
        limit: Number of model entities to be fetched      
        offset: The number of results to skip.
        _cache: Cache layers to retrieve the results. If no match is found
          the query is run on datastore and these layers are refreshed.         
        _local_expiration: Expiration in seconds for local cache layer, if 
          a cache refresh operation is run.         
        _memcache_expiration: Expiration in seconds for memcache,
          if a cache refresh operation is run.
        
      Returns:
        The return value is a list of model instances, possibly an empty list.
      
      Raises:
        CacheLayerError: If an invalid cache layer name is supplied
      '''
      klass = self.__class__
      if _cache is None:
        _cache = []
      else:
        _cache = _to_list(_cache)
        _validate_cache(_cache)

      result = None
      
      local_flag = True if LOCAL in _cache else False
      memcache_flag = True if MEMCACHE in _cache else False
        
      self._clear_keyname(klass.offset_key)
      self._clear_keyname(klass.limit_key)
      self._concat_keyname(klass.limit_key+str(limit))
      if offset != 0:
        self._concat_keyname(klass.offset_key+str(offset))

      if local_flag:
        result = cachepy.get(self.key_name)

      if memcache_flag and result is None:
        result = _deserialize(memcache.get(self.key_name))
        if local_flag and result is not None:
          cachepy.set(self.key_name,result,_local_expiration)
      
      if result is None:
        result = self.query.fetch(limit,offset)
        if memcache_flag:
          memcache.set(self.key_name,_serialize(result),_memcache_expiration)
        if local_flag:
          cachepy.set(self.key_name,result,_local_expiration)
      
      return result
        
class time_util(object):
  '''This is a utility class for using update periods for cache invalidation
  
    Example Usage:
      Assume that we have 3 models (MinuteModel,HourModel,DayModel)
      with update frequencies of 10 minutes, 1 hour and 1 day.
      
      We want to store and serve them using local cache, so we have to 
      make sure that their local storage is invalidated after their update
      frequency time is passed.
      
      minute_model = MinuteModel()
      hour_model = HourModel()
      day_model = DayModel()
      
      #Assume it is 15:23:10 at the time of following operations
      
      #Expiration time: 15:30:00 - 15:23:10 = 410 seconds
      minute_model.put(_storage=['local','datastore'],
                              _local_expiration = time_util.minute_expiration(minutes=10))
      
      #Expiration time: 16:00:00 - 15:23:10 = 2210 seconds
      hour_model.put(_storage=['local','datastore'],
                            _local_expiration = time_util.hour_expiration(hours=1))
      
      #Expiration time: 00:00:00 - 15:23:10 = 31010 seconds
      day_model.put(_storage=['local','datastore'],
                          _local_expiration = time_util.day_expiration(days=1))
  '''
  @classmethod
  def now(cls):
    return datetime.utcnow()
  
  @classmethod
  def today(cls):
    now = cls.now()
    return date(now.year,now.month,now.day)
  
  @classmethod
  def minute_expiration(cls,minutes=10,
                                        minute_offset=0,
                                        _test_datetime=None):
    '''Returns seconds left for the next minute period
    Starting at minute_offset'''
    if _test_datetime:
      now = _test_datetime
    else:
      now = cls.now()
    second = now.second
    minute = now.minute
    if minute < minutes + minute_offset:
      elapsed = minute*60
    else:
      elapsed = (minute % minutes)*60+second
    return (minutes+minute_offset)*60-elapsed
  
  @classmethod
  def hour_expiration(cls,hours=1,hour_offset=0,
                                    minute_offset=0,
                                    _test_datetime=None):
    '''Returns seconds left for the next hour period
    Starting at hour_offset:minute_offset'''
    if _test_datetime:
      now = _test_datetime
    else:
      now = cls.now()
    second = now.second
    minute = now.minute
    hour = now.hour
    elapsed = (hour % hours)*3600+minute*60+second
    return (hours+hour_offset)*3600+minute_offset*60-elapsed
    
  @classmethod
  def day_expiration(cls,days=1,day_offset=0,
                                   hour_offset=0,
                                   minute_offset=0,
                                   _test_datetime=None):
    '''Returns seconds left for the next day period
    Starting at day_offset:hour_offset:minute_offset'''
    if _test_datetime:
      now = _test_datetime
    else:
      now = cls.now()
    second = now.second
    minute = now.minute
    hour = now.hour
    day = now.day
    elapsed = (day % days)*86400+hour*3600+minute*60+second
    return (days+day_offset)*86400+hour_offset*3600+minute_offset*60-elapsed
    
class _ReferenceCacheIndex(pdb.Model):
  '''This model is used for accessing the 'many' part of a 
  one-to-many relationship that uses db.ReferenceProperty
  through cache, instead of running a db.Query. 
  
  An instance of this class is saved into memcache
  when 'cached_set' method of a pdb.Model is called.
  '''
  ref_keys = db.ListProperty(db.Key,indexed = False)
  
  @classmethod
  def create(cls,reference,collection_name,
             models,_memcache_expiration):
    entity = cls(key_name=str(reference.key())+cls._default_delimiter+collection_name)
    entity.ref_keys = [model.key() for model in models]
    entity.put(_storage=MEMCACHE,
               _memcache_expiration = _memcache_expiration)    
    return entity

class ResultTypeError(Exception):
  def __init__(self,type):
    self.type = type
  def __str__(self):
    return  'Result type is invalid: %s. Valid values are "list" and "dict" and "name_dict"' %self.type
  
class ReferenceError(Exception):
  COLLECTION_NAME_ERROR = 'Entity does not have a reference set called: '
  REFERENCE_NAME_ERROR = 'Entity does not have a reference property called: '
  TYPE_ERROR = 'Expected ReferenceProperty but received: '
  
  def __init__(self,param,message):
    self.param = param
    self.message = message
  def __str__(self):
    return  self.message+str(self.param)
  
class CacheLayerError(Exception):
  def __init__(self,cache):
    self.cache = cache
  def __str__(self):
    return  'Cache layer name invalid: %s. Valid values are "local" and "memcache"' %self.cache
  
class StorageLayerError(Exception):
  def __init__(self,storage):
    self.storage = storage
  def __str__(self):
    return  'Storage layer name invalid: %s. Valid values are "local","memcache" and "datastore"' %self.storage

class KeyParameterError(Exception):
  def __init__(self,param):
    self.type = type(param)
  def __str__(self):
      return  '%s was given as function parameter, it should be db.Key,String or db.Model' %self.type
       
class IdentifierNotFoundError(Exception):
    def __str__(self):
        return  'Error trying to write models into cache without valid identifiers. Try enabling datastore write for the models or use keynames instead of IDs.'
