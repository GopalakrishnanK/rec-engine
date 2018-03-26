import os
import json
from flask import Flask, request, redirect, url_for, Response, session
from werkzeug.utils import secure_filename
from flask.views import View, MethodView
import settings
import middleware
from bson.json_util import dumps
import uuid
from flask.ext.login import login_required
# from utils import login_required
from pyspark import SparkContext
from pyspark.sql import SQLContext
from pyspark.sql import *
from pyspark.sql.types import *
from pyspark.sql.functions import *
from flask import jsonify
# from flask_googlelogin import GoogleLogin
from pyspark.mllib.recommendation import ALS, MatrixFactorizationModel, Rating


APP_ROOT = os.path.dirname(os.path.abspath(__file__))
application = Flask(__name__)
UPLOAD_FOLDER = APP_ROOT + "/uploads"


ALLOWED_EXTENSIONS = set(['csv'])
application.config['UPLOAD_FOLDER'] = UPLOAD_FOLDER

sc = SparkContext("local", "RecEngine")
sqlContext = SQLContext(sc)


#Creating flask application with configurations
application.config.from_object(settings.ProductionConfig)

#Middleware apply for all the Req and Res

#application.wsgi_app = middleware.SimpleMiddleWare(application.wsgi_app)

#MongoDB connection and mongo object
mongo = settings.get_mongo(application)

#Google authentications
googlelogin = GoogleLogin(application)
uid = str(uuid.uuid4())

#Google authentication
# @application.route('/oauth2callback')
# @googlelogin.oauth2callback

def create_or_update_user(token, userinfo, **params):
    #User details after login
    print userinfo['id']
    session['access_token'] = token["access_token"]
    session['id'] = userinfo['id']
    return redirect(url_for('upload_csv'))

def flash_message_construct(message):
    return dumps({"message": message})

def allowed_file(filename):
    return '.' in filename and \
           filename.rsplit('.', 1)[1].lower() in ALLOWED_EXTENSIONS

#Dynamic URL Mapping
def register_api(view, endpoint, url, pk='id', pk_type='string'):
    view_func = view.as_view(endpoint)
    application.add_url_rule(url, defaults={pk: None},
                     view_func=view_func, methods=['GET',])
    application.add_url_rule(url, view_func=view_func, methods=['POST',])
    application.add_url_rule('%s<%s:%s>' % (url, pk_type, pk), view_func=view_func,
                     methods=['GET', 'PUT', 'DELETE'])


class UploadAPI(MethodView):
    # @login_required 
    def get(self):
        return '''
        <!doctype html>
        <title>Upload new File</title>
        <h1>Upload new File</h1>
        <form method=post enctype=multipart/form-data>
          <p><input type=file name=file>
             <input type=submit value=Upload>
        </form>
        '''
    # @login_required 
    def post(self):
        file = request.files['file']
        if file.filename == '':
            raise Exception("No file found")
            return redirect(request.url)
        if file and allowed_file(file.filename):
            filename = secure_filename(file.filename)
            filepath = os.path.join(application.config['UPLOAD_FOLDER'], filename)
            if not os.path.exists(os.path.join(application.config['UPLOAD_FOLDER'])):
                os.makedirs(os.path.join(application.config['UPLOAD_FOLDER']))
            file.save(filepath)
            data = {'_id':uid,'filepath':filepath}
            users = mongo.db.file_data.save(data)
            return '''
            <!doctype html>
            <title>Get Summary</title>
            <h1>Get Summary</h1>
            <form action="/summary/'''+uid+'''" method="get">
            <input type="submit" value="Submit">
            </form>
            '''
application.add_url_rule('/upload/', view_func=UploadAPI.as_view('upload_csv'))

class SummaryAPI(MethodView):
    # @login_required 
    def get(self,user_id):
        data = mongo.db.file_data.find_one({"_id": user_id})
        filepath = data['filepath']
        Transaction_Mockup = sqlContext.read.csv(filepath,header='true')
        Transaction_Mockup = Transaction_Mockup.withColumn('Gross_Profit',col('Net_Sales')-col('Net_Cost'))
        Unique_Customers = Transaction_Mockup.select('Customer_Number').drop_duplicates().count()
        Unique_Items = Transaction_Mockup.select('Item Number').drop_duplicates().count()
        Unique_States = Transaction_Mockup.select('Customer_State').drop_duplicates().count()
        Total_Sales_Profit = Transaction_Mockup.agg(sum("Gross_Profit").alias("Gross_Profit"),sum('Net_Sales').alias('Sum_Sales'),sum('Cases_Sold').alias('Cases_Sold')).collect()
        Item_Category_Split = Transaction_Mockup.groupBy('Item_Category').agg(sum("Gross_Profit").alias("Gross_Profit"),sum('Net_Sales').alias('Sum_Sales'),sum('Cases_Sold').alias('Cases_Sold'))
        response = {}
        response['Unique_Customers'] = Unique_Customers
        response['Unique_Items'] = Unique_Items
        response['Unique_States'] = Unique_States
        response['Total_Sales'] = Total_Sales_Profit[0]['Sum_Sales']
        response['Total_Profit'] = Total_Sales_Profit[0]['Gross_Profit']
        response['Total_Cases_Sold'] = Total_Sales_Profit[0]['Cases_Sold']
        response['_id'] = uid
        data_resp = mongo.db.summary.save(response)
        return '''
            <!doctype html>
            <title>Get Summary</title>
            <h1>View Summary</h1>
            <form action="/view_summary/'''+uid+'''" method="get">
            <input type="submit" value="Submit">
            </form>
            '''

application.add_url_rule('/summary/<string:user_id>', view_func=SummaryAPI.as_view('summary_api'))


class View_SummaryAPI(MethodView):
    # @login_required 
    def get(self,user_id):
        data = mongo.db.summary.find_one({"_id": user_id})
        return json.dumps(data)

application.add_url_rule('/view_summary/<string:user_id>', view_func=View_SummaryAPI.as_view('view_summary_api'))

class getRec(MethodView):
    # @login_required 
    def get(self,user_id):
        data = mongo.db.file_data.find_one({"_id": user_id})
        filepath = data['filepath']
        Transaction_Mockup = sqlContext.read.csv(filepath,header='true')
        ratings = Transaction_Mockup.select('Customer_Number').drop_duplicates().crossJoin(Transaction_Mockup.select('Item Number').drop_duplicates()).join(Transaction_Mockup.select('Customer_Number','Item Number').drop_duplicates().withColumn('Reference_Column',lit(1)),['Customer_Number','Item Number'],'left').na.fill({'Reference_Column': 0})#.rdd
        rank = 100
        numIterations = 40
        ratings_rdd = ratings.rdd
        model = ALS.train(ratings_rdd, rank, numIterations,nonnegative = True)
        testdata = ratings_rdd.map(lambda p: (p[0], p[1]))
        predictions = model.predictAll(testdata).toDF()
        predictions = predictions.withColumnRenamed("user","Customer_Number").withColumnRenamed("product","Item Number")
        predictions = predictions.withColumn("rank", dense_rank().over(Window.partitionBy(col("Customer_Number")).orderBy(col("rating").desc()))).filter(col('rank')<=5)
        return predictions

application.add_url_rule('/get_rec/<string:user_id>', view_func=getRec.as_view('recommendations_api'))

#AUTHENTICATION SECTION

@application.route('/')
def index():
    return redirect('/upload')

@application.route('/logout')
def logout():
    session.clear()
    return redirect('/upload')

# @application.route('/login')
# def login():
#     return """
#         <p><a href="%s">Login</p>
#     """ % (
#         googlelogin.login_url(approval_prompt='force')
    # )

if __name__ == '__main__':
	application.run(host='0.0.0.0',port=8585,debug = True,threaded=True)


	
