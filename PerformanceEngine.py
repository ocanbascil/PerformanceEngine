from google.appengine.api import memcache
from google.appengine.ext import db
from google.appengine.datastore import entity_pb
from google.appengine.ext import deferred

import cachepy
import logging

DEFAULT_NAMESPACE = 'default_namespace'

'''Constants for storage levels'''
DATASTORE = 'datastore'
MEMCACHE = 'memcache'
LOCAL = 'local'

ALL_LEVELS = [DATASTORE,MEMCACHE,LOCAL]

'''Constants for result types'''
LIST = 'list'
DICT = 'dict'

LOCAL_EXPIRATION = 0
MEMCACHE_EXPIRATION = 0

def key_str(param):
  if isinstance(param, db.Model):
    return str(param.key())
  elif isinstance(param, db.Key):
    return str(param)
  elif isinstance(param,str):
    return param
  else:
    raise KeyParameterError(param)

def _diff(list1,list2):
  '''Finds the difference of keys between 2 lists
  Used for layered model retrieval'''
  return list(set(list1)-set(list2))

def _to_list(param): 
    if not type(param).__name__=='list':
        result = []
        result.append(param)
    else:
        result = list(param)
    return result

def _to_dict(models):
  '''Utility method to create identifier:model dictionary'''
  result = {}
  for model in models:
    result[key_str(model)] = model
  return result

def serialize(models):
  '''Improve memcache performance converting to protobuf'''
  if models is None:
    return None
  elif isinstance(models, db.Model):
    # Just one instance
    return db.model_to_protobuf(models).Encode()
  else:
    # A list
    return [db.model_to_protobuf(x).Encode() for x in models]

def deserialize(data):
  '''Improve memcache performance by converting from protobuf'''
  if data is None:
    return None
  elif isinstance(data, str):
    # Just one instance
    return db.model_from_protobuf(entity_pb.EntityProto(data))
  else:
    return [db.model_from_protobuf(entity_pb.EntityProto(x)) for x in data]

def _cachepy_get(keys):
  '''Get items with given keys from local cache
  
  Args:
    keys: List of db.Keys or string representation of db.Keys
  
  Returns:
    Dictionary of key,model pairs in which keys are 
    string representation of db.Key instances
  '''
  result = {}
  for key in keys:
    result[key] = deserialize(cachepy.get(key))
  return result

def _cachepy_put(models,time = 0):
  '''Put given models to local cache in serialized form
   with expiration in seconds
  
  Args:
    models: List of models to be saved to local cache
    time: Expiration time in seconds for each model instance
  
  Returns:
    List of string representations of db.Keys 
    of the models that were put
    
    If no model is found for given key, value for that key
    in result is set to None
  '''
  to_put = _to_dict(models)
  if time == 0: #cachepy uses None as unlimited caching flag
    time = None
  
  for key, model in to_put.iteritems():
    cachepy.set(key,serialize(model),time)
  return to_put.keys()


def _cachepy_delete(keys):
    for key in keys: 
        cachepy.delete(key)

def _memcache_get(keys):
  '''Get items with given keys from memcache
  
  Args:
    keys: List of db.Keys or string representation of db.Keys
  
  Returns:
    Dictionary of key,model pairs in which keys are 
    string representation of db.Key instances
    
    If no model is found for given key, value for that key
    in result is set to None
  '''
  cache_results = memcache.get_multi(keys)
  result = {}
  for key in keys:
    try:
      result[key] = deserialize(cache_results[key])
    except KeyError:
      result[key] = None
  return result
    
def _memcache_put(models,time = 0):
  '''Put given models to memcache in serialized form
   with expiration in seconds
  
  Args:
    models: List of models to be saved to local cache
    time: Expiration time in seconds for each model instance
  
  Returns:
    List of string representations of db.Keys 
    of the models that were put
  '''         
  to_put = _to_dict(models)
        
  for key,model in to_put.iteritems():
      to_put[key] = serialize(model)
          
  memcache.set_multi(to_put,time)
  return to_put.keys()


def _memcache_delete(keys): #Seconds for lock?
    memcache.delete_multi(keys)

  
class pdb(object):
  
  @classmethod
  def get(cls,keys,_storage = [LOCAL,MEMCACHE,DATASTORE],**kwargs):
    """Fetch the specific Model instance with the given key from given storage layers.
  
    Args:
      _storage: string or array of strings for target storage layers  
      
      Inherited:
        keys: Key within datastore entity collection to find; or string key;
          or list of Keys or string keys.
        config: datastore_rpc.Configuration to use for this request.
      
    Returns:
      If a single key was given: a Model instance associated with key
      for if it exists in the datastore, otherwise None; if a list of
      keys was given: a list whose items are either a Model instance or
      None.
    """
    none_filter  = lambda dict : [k for k,v in dict.iteritems() if v is None]
    
    keys = _to_list(keys)
    keys = map(key_str, keys)
    old_keys = keys
    not_found  = []
    result = []
    models = {}
    
    if LOCAL in _storage:
        #print 'Getting from local storage, keys: %s' %keys
        models = dict(models,**_cachepy_get(keys))
        keys = none_filter(models)
        print '1',models
        not_found = _diff(keys, models.keys())
        if len(not_found):
          keys = not_found
          
    if MEMCACHE in _storage and len(keys):
        models = dict(models,**_memcache_get(keys))
        keys = none_filter(models)
        print '2',models
        not_found = _diff(keys,models.keys())
        if len(not_found):
          keys = not_found
    
    if DATASTORE in _storage and len(keys):
        db_results = [key for key in db.get(keys) if key is not None]
        #print 'db_results %s'%db_results
        if len(db_results):
          models  = dict(models,**_to_dict(db_results))
          #print 'models in datastore %s' %models
        print '3',models
      
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
        
  @classmethod
  def put(cls,models,_storage = ALL_LEVELS,
                      _local_expiration = LOCAL_EXPIRATION,
                      _memcache_expiration = MEMCACHE_EXPIRATION,
                       **kwargs):
    '''Saves models into given storage layers and returns their keys
    
    If the models are written for the first time and they have no keys ,
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
      IdentifierNotFoundError if models with no valid identifiers 
      are written into cache storage only
    
      Inherited:
        TransactionFailedError if the data could not be committed.
    '''

    keys = [] 
    models = _to_list(models)   
    _storage = _to_list(_storage)
    
    try: 
      _to_dict(models)
    except db.NotSavedError:
      if DATASTORE in _storage:
        keys = db.put(models)
        models = db.get(keys)
        _storage.remove(DATASTORE)
        if len(_storage):
          return pdb.put(models,_storage,_local_expiration,_memcache_expiration)
      else: 
        raise IdentifierNotFoundError() 
    
    if DATASTORE in _storage:
      keys = db.put(models)
      
    if LOCAL in _storage:
      keys = _cachepy_put(models, _local_expiration)

    if MEMCACHE in _storage:
      keys = _memcache_put(models,_memcache_expiration)
      
    return keys

  
  @classmethod
  def delete(cls,keys,_storage = ALL_LEVELS):
    keys = map(key_str,keys)
      
    if DATASTORE in _storage:
      db.delete(keys)
      
    if LOCAL in _storage:
      _cachepy_delete(keys)

    if MEMCACHE in _storage:
      _memcache_delete(keys)
  
  class Model(db.Model):
    '''Wrapper class for db.Model
    Adds cached storage support to common functions'''
    
    def put(self,**kwargs):
      return pdb.put(self, **kwargs)
    
    def get(self,keys,**kwargs):
      return pdb.get(keys,**kwargs)
    
    def delete(self,_storage = ALL_LEVELS):
      pass
    
    def get_or_insert(self,key_name,**kwargs):
      #Add get without txn here
      pass
    
    @classmethod
    def get_by_key_name(cls,key_name):
      pass
    
    @classmethod
    def get_by_id(cls,id):
      pass
    ''' 
    def get(self,keys,
                    _storage = _storage,
                    **kwargs):
        result = []
        if LOCAL in _storage:
            pass
        
        if MEMCACHE in _storage:
            pass
        
        if DATASTORE in _storage:
            pass
    
    def delete(self,
               _storage_layers =_storage,
                **kwargs):
        
        if LOCAL in _storage_layers:
            pass
        
        if MEMCACHE in _storage_layers:
            pass
        
        if DATASTORE in _storage_layers:
            pass
    
    def put(self,_storage_layers =_storage,
                    _memcache_expiration = _memcache_expiration,
                    _local_expiration = _local_cache_expiration,
                    **kwargs):
        
        if LOCAL in _storage_layers:
            pass
        
        if MEMCACHE in _storage_layers:
            pass
        
        if DATASTORE in _storage_layers:
            pass
        
        #self.before_put()
        super(Model, self).put(**kwargs)
        #self.after_put()
     '''     
    def clone_entity(self,**extra_args):
        """Clones an entity, adding or overriding constructor attributes.
        
            The cloned entity will have exactly the same property values as the original
            entity, except where overridden. By default it will have no parent entity or
            key name, unless supplied.
        
        Args:
            e: The entity to clone
            extra_args: Keyword arguments to override from the cloned entity and pass
            to the constructor.
        Returns:
            A cloned, possibly modified, copy of entity e.
        """
        klass = self.__class__
        props = {}
        
        for k,v in klass.properties().iteritems():
            if isinstance(v, db.ReferenceProperty):
                props[k] = v.get_value_for_datastore(self)
            else:
                props[k] = v.__get__(self,klass)
                
        props.update(extra_args)
        return klass(**props)
    
    def log_properties(self):
        
        for k,v in self.properties().iteritems():
            logging.info('%s : %s' %(k,v.get_value_for_datastore(self)))  
  
class GqlQuery(db.GqlQuery):
    pass



    




class KeyParameterError(Exception):
  def __init__(self,param):
      self.type = type(param)
  def __str__(self):
      return  '%s was given as function parameter, it should be db.Key,String or db.Model' %self.type
       
            
class IdentifierNotFoundError(Exception):
    def __str__(self):
        return  'Error trying to write models into cache without valid identifiers. Try enabling datastore write for the models or use keynames instead of IDs.'
