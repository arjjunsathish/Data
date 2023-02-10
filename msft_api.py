#This module contains the code to develop a microservice. The microservice will decouple any Machine Learning model form the Data Pipeline. It is essential to decouple the ML model from the Data pipeline because we expect 
#frequent changes in the learning model, as a result of which we might end in deploying the entire pipeline along with the ML model if they are not decouple4d. Hence we segregate the pipeline with the ML model using the Microservice. 
#In order to develop the microservice we use Flask and jsonify modules. 
import ast
import mysql
from flask import Flask
from flask_restful import Resource, Api, reqparse
import pandas as pd


#Establish DB connection
mydb = mysql.connector.connect(host = "localhost", user = "", password = "", auth_plugin='mysql_native_password')
mycursor = mydb.cursor()


class Pull20DaysMovingAverage(Resources):
    def post(self):
        parser = reqparse.RequestParser()  # initialize
        parser.add_argument('stockname', required=True)  
        mycursor.execute("select *from Thales.20DaysRollingAverage where StockName = args['stockname'];")
        data = mycursor.fetchall()
        return {'data': data.to_dict()}, 200  # return data with 200 OK

class Pull50DaysMovingAverage(Resources):
    def 20Days(self):
        parser = reqparse.RequestParser()  # initialize
        parser.add_argument('stockname', required=True)  
        mycursor.execute("select *from Thales.50DaysRollingAverage where StockName = args['stockname'];")
        data = mycursor.fetchall()
        return {'data': data.to_dict()}, 200  # return data with 200 OK

class Pull200DaysMovingAverage(Resources):
    def 20Days(self):
        parser = reqparse.RequestParser()  # initialize
        parser.add_argument('stockname', required=True)  
        mycursor.execute("select *from Thales.200DaysRollingAverage where StockName = args['stockname'];")
        data = mycursor.fetchall()
        return {'data': data.to_dict()}, 200  # return data with 200 OK


app = Flask(__name__)
api = Api(app)

api.add_resource(Pull20DaysMovingAverage, '/Pull20DaysMovingAverage')
api.add_resource(Pull50DaysMovingAverage, '/Pull50DaysMovingAverage')
api.add_resource(Pull200DaysMovingAverage, '/Pull200DaysMovingAverage')


if __name__ == '__main__':
    app.run()  # run our Flask app