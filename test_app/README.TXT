This sample application demonstrates the usage of GAEUnit.

Before running the application, copy 'gaeunit.py' from the directory above to this directory
(or create a symbolic link).

The sample demonstrates the three different ways to invoke tests:
  
  default:
    http://localhost:8080/test
    runs all tests under the 'test' directory

  by test name:
    http://localhost:8080/test?name=unit_tests
    runs only the tests in 'test/unit_tests.py'
  
  by package name:
    http://localhost:8080/test?package=packaged_tests
    runs all tests in the 'packaged_tests' module, as specified in 'packaged_tests/__init__.py'

The 'webtest' module has been included with a slight modification.  In 'webtest/__init__.py', the
line 'import webbrowser' was commented out since the Google App Engine environment does not
include this module.
