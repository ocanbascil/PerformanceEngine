import unittest
from google.appengine.ext import db
from google.appengine.api import memcache
from google.appengine.ext import testbed
from PerformanceEngine import pdb,time_util
from models import PdbModel
from datetime import datetime


class GetTest(unittest.TestCase):

  def setUp(self):
    self.testbed = testbed.Testbed()
    self.testbed.activate()
    self.testbed.init_datastore_v3_stub()
    self.testbed.init_memcache_stub()
    self.entity = PdbModel(key_name='test')
    self.time = datetime(2011,1,1,0,0,0)
    
    
  def tearDown(self):
    self.time = datetime(2011,1,1,0,0,0)
    self.testbed.deactivate()

  def test_minute_expiration(self):
    default_exp =  time_util.minute_expiration(_test_datetime=self.time)
    value_exp = time_util.minute_expiration(25,_test_datetime=self.time)
    offset_exp = time_util.minute_expiration(minute_offset=3,_test_datetime=self.time)
    value_offset_exp = time_util.minute_expiration(25,minute_offset=3,_test_datetime=self.time)
 
    self.assertEqual(default_exp,600)
    self.assertEqual(value_exp,1500)
    self.assertEqual(offset_exp,780) #10+3
    self.assertEqual(value_offset_exp,1680) #25+3
    
    self.time = datetime(2011,1,1,0,12,0)
    default_exp =  time_util.minute_expiration(_test_datetime=self.time)
    value_exp = time_util.minute_expiration(25,_test_datetime=self.time)
    offset_exp = time_util.minute_expiration(minute_offset=3,_test_datetime=self.time)
    value_offset_exp = time_util.minute_expiration(25,minute_offset=3,_test_datetime=self.time)
    
    self.assertEqual(default_exp,480)
    self.assertEqual(value_exp,780)
    self.assertEqual(offset_exp,60) 
    self.assertEqual(value_offset_exp,960) 
    

  
  def test_hour_expiration(self):
    default_exp =  time_util.hour_expiration(_test_datetime=self.time)
    value_exp = time_util.hour_expiration(5,_test_datetime=self.time)
    minute_offset_exp = time_util.hour_expiration(minute_offset=30,_test_datetime=self.time)
    hour_offset_exp = time_util.hour_expiration(hour_offset=2,_test_datetime=self.time)
    hour_minute_offset_exp = time_util.hour_expiration(hour_offset=2,minute_offset=30,_test_datetime=self.time)
    value_hour_offset_exp = time_util.hour_expiration(5,hour_offset=2,_test_datetime=self.time)
    value_hour_minute_offset_exp = time_util.hour_expiration(5,hour_offset=2,minute_offset=30,_test_datetime=self.time)
    
    self.assertEqual(default_exp,3600)
    self.assertEqual(value_exp,18000)
    self.assertEqual(minute_offset_exp,5400)
    self.assertEqual(hour_offset_exp,10800) #Problem here 
    self.assertEqual(hour_minute_offset_exp,12600) 
    self.assertEqual(value_hour_offset_exp,25200)    
    self.assertEqual(value_hour_minute_offset_exp,27000)     
    
    self.time = datetime(2011,1,1,3,30,0)

    default_exp =  time_util.hour_expiration(_test_datetime=self.time)
    value_exp = time_util.hour_expiration(5,_test_datetime=self.time)
    minute_offset_exp = time_util.hour_expiration(minute_offset=30,_test_datetime=self.time)
    hour_offset_exp = time_util.hour_expiration(hour_offset=2,_test_datetime=self.time)
    hour_minute_offset_exp = time_util.hour_expiration(hour_offset=2,minute_offset=30,_test_datetime=self.time)
    value_hour_offset_exp = time_util.hour_expiration(5,hour_offset=2,_test_datetime=self.time)
    value_hour_minute_offset_exp = time_util.hour_expiration(5,hour_offset=2,minute_offset=30,_test_datetime=self.time)
    
    print 'default_exp %s' %default_exp
    print 'value_exp %s' %value_exp
    print 'minute_offset_exp %s' %minute_offset_exp
    print 'hour_offset_exp %s' %hour_offset_exp
    print 'hour_minute_offset_exp %s' %hour_minute_offset_exp
    print 'value_hour_offset_exp %s' %value_hour_offset_exp
    print 'value_hour_minute_offset_exp %s' %value_hour_minute_offset_exp
    
    
  def test_day_expiration(self):
    print time_util.day_expiration(1,_test_datetime=self.time)