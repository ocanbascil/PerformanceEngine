from google.appengine.ext import webapp
from google.appengine.ext.webapp.util import run_wsgi_app

class IndexHandler(webapp.RequestHandler):
    def get(self):
        name = self.request.get('name', 'World')
        self.response.out.write("<html><body>Hello, %s!</body></html>" % name)


def main():
    application = webapp.WSGIApplication([('/', IndexHandler)], debug=True)
    run_wsgi_app(application)                                    

if __name__ == '__main__':
    main()
