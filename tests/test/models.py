from google.appengine.ext import db
from PerformanceEngine import pdb

class PdbModel(pdb.Model):
  name = db.StringProperty()
  count = db.IntegerProperty()
  
class TestModel(db.Model):
  name = db.StringProperty()