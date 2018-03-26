from flask_pymongo import PyMongo

class Config(object):
    DEBUG = True
    TESTING = False
    SECRET_KEY='Miengous3Xie5meiyae6iu6mohsaiRae'
    GOOGLE_LOGIN_CLIENT_ID='253885441547-8dim98m1cai0ph9694uhrkcf3cshnk1g.apps.googleusercontent.com'
    GOOGLE_LOGIN_CLIENT_SECRET='RecV1Pq_nMjJf9EiSooF_Qy9'
    GOOGLE_LOGIN_REDIRECT_URI='http://localhost:8585/oauth2callback'

class ProductionConfig(Config):
    DEBUG = False

class DevelopmentConfig(Config):
    DEBUG = True

class TestingConfig(Config):
    TESTING = True

def get_mongo(app):
	app.config['MONGO_HOST'] = 'mongodb://localhost'
	app.config['MONGO_PORT'] = 27017
	app.config['MONGO_DBNAME'] = 'test'
	return PyMongo(app, config_prefix='MONGO')
