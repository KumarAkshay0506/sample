from flask import Flask, render_template, request, redirect, url_for, session
from azure.storage.blob import BlockBlobService
from azure.storage.blob import PublicAccess
from azure.storage.blob import ContentSettings
import mysql.connector
from mysql.connector import errorcode
import os
import csv
import random
from datetime import datetime
from redis import Redis
import redis
import six
from six.moves import cPickle as pickle

app = Flask(__name__)
#clone successfull
config = {
  'host':'myserver-mysql-ashu.mysql.database.azure.com',
  'user':'root123@myserver-mysql-ashu',
  'password':'Superman123',
  'database':'mysqlashudb',
  'ssl_ca':'BaltimoreCyberTrustRoot.crt.pem'
}

r_server = redis.StrictRedis(host='ashuredis.redis.cache.windows.net', port=6380, db=0, password='RgqQTepeV40zxbF8pTcMLuP5KwStyBCJ++Fb7c1J1UM=', ssl=True)

#block_blob_service = BlockBlobService(account_name='ashuazurestorage', account_key='HGvsHgPPFOp64gztvR6B9g+RNUUqzwhl+aNid8wpwca1uwejBMEhyVkP3oev1SKEnI5eeq4EIXWfcvzWjxAjuQ==')
#block_blob_service.set_container_acl('ashu-blob-container', public_access=PublicAccess.Container)

@app.route('/searchByCityQuery', methods=['GET','POST'])
def searchByCityQuery():
    if request.method == 'POST':
        city = request.form['city']
        print("city %s " % (city))
        time_start = datetime.now()
        key = city;
        if (r_server.get(key)):
            print("inside")
            results = pickle.loads(r_server.get(key))
            print(results)
            time_end = datetime.now()
            time_diff = time_end - time_start
            timediff = str(time_diff)
            session['time_diff'] = timediff
            return render_template('CitySearchResult.html', results=results)
        else:
            try:
                     conn = mysql.connector.connect(**config)
                     print("Connection established")
            except mysql.connector.Error as err:
                    if err.errno == errorcode.ER_ACCESS_DENIED_ERROR:
                      print("Something is wrong with the user name or password")
                    elif err.errno == errorcode.ER_BAD_DB_ERROR:
                      print("Database does not exist")
                    else:
                      print(err)
            else:
                    cursor = conn.cursor()
                    cursor.execute("SELECT givenName, surName, city, state FROM people_table WHERE city = %s ;", [city])
                    results = cursor.fetchall()
                    cursor.close()
                    conn.close()
            time_end = datetime.now()
            time_diff = time_end - time_start
            timediff = str(time_diff)
            print("inside else")
            session['time_diff'] = timediff
            r_server.set(key,pickle.dumps(results))
            r_server.expire(key, 36)
            return render_template('CitySearchResult.html', results=results)
    return render_template('searchByCityQuery.html')

@app.route('/')
def index():
  return redirect(url_for('login'))

@app.route('/login', methods=['POST', 'GET'])
def login():
    if request.method == 'POST':
        username = request.form['username']
        session['logged_in'] = True
        session['username'] = username
        time_start = datetime.now()
        session['time'] = time_start
        # bar = ['hello', 'dear', 'how', 'are', 'you']
        # r_server.set('foo', pickle.dumps(bar))
        # res = pickle.loads(r_server.get('foo'))
        return redirect(url_for('dashboard'))
    return render_template('login.html')

@app.route('/randomQueries')
def randomQueries():
  return render_template('randomQueries.html')

@app.route('/dashboard')
def dashboard():
    return render_template('dashboard.html')

@app.route('/random1000queries')
def random1000queries():
    try:
             conn = mysql.connector.connect(**config)
             print("Connection established")
    except mysql.connector.Error as err:
            if err.errno == errorcode.ER_ACCESS_DENIED_ERROR:
              print("Something is wrong with the user name or password")
            elif err.errno == errorcode.ER_BAD_DB_ERROR:
              print("Database does not exist")
            else:
              print(err)
    else:
            cursor = conn.cursor()
            time_start = datetime.now()
            for i in range(0,200):
                randomnum = random.uniform(0,1)
                result = cursor.execute("SELECT mag FROM earthquake_table WHERE mag >= %s AND mag <= %s ;", (randomnum,randomnum+1.0))
                articles = cursor.fetchall()
            cursor.close()
            conn.close()
            time_end = datetime.now()
    time_diff = time_end - time_start
    return render_template('complete.html', time_diff=time_diff)

@app.route('/restrictedQueries')
def restrictedQueries():
    return render_template('restrictedQueries.html')

@app.route('/searchByRangeQuery', methods=['GET','POST'])
def searchByRangeQuery():
    if request.method == 'POST':
        latitudeStart = request.form['latitudeStart']
        latitudeEnd = request.form['latitudeEnd']
        age_lower = request.form['age_lower']
        age_upper = request.form['age_upper']
        #print("latitude %s " % (latitude))
        time_start = datetime.now()
        try:
                 conn = mysql.connector.connect(**config)
                 print("Connection established")
        except mysql.connector.Error as err:
                if err.errno == errorcode.ER_ACCESS_DENIED_ERROR:
                  print("Something is wrong with the user name or password")
                elif err.errno == errorcode.ER_BAD_DB_ERROR:
                  print("Database does not exist")
                else:
                  print(err)
        else:
                cursor = conn.cursor()
                cursor.execute("SELECT givenName, surName, city, state, Age, latitude, longitude FROM people_table WHERE (latitude >= %s AND  latitude <= %s) AND (Age >= %s AND Age <= %s) ;", (latitudeStart, latitudeEnd, age_lower, age_upper))
                results = cursor.fetchall()
                cursor.close()
                conn.close()
                count = 0;
                for result in results:
                    count = count + 1
                time_end = datetime.now()
                time_diff = time_end - time_start
                timediff = str(time_diff)
                session['time_diff'] = timediff
                session['result_count'] = str(count)
                return render_template('RangeSearchResult.html', results=results)
    return render_template('SearchByRange.html')



@app.route('/createDB')
def createDB():
  fileread = open('data.csv','rt')
  file_reader = csv.reader(fileread)
  dataPeople = []
  try:
           conn = mysql.connector.connect(**config)
           print("Connection established")
  except mysql.connector.Error as err:
          if err.errno == errorcode.ER_ACCESS_DENIED_ERROR:
            print("Something is wrong with the user name or password")
          elif err.errno == errorcode.ER_BAD_DB_ERROR:
            print("Database does not exist")
          else:
            print(err)
  else:
          cursor = conn.cursor()
          time_start = datetime.now()
          cursor.execute("DROP TABLE IF EXISTS people_table;")
          cursor.execute("CREATE TABLE people_table (ID INT NOT NULL AUTO_INCREMENT, gender VARCHAR(10), givenName VARCHAR(25), surName VARCHAR(25),  streetAddress VARCHAR(50), city VARCHAR(25), state VARCHAR(10), emailAddress VARCHAR(100), username VARCHAR(50), telephoneNumber VARCHAR(15), Age INT, bloodType VARCHAR(10), centimeters INT, latitude DOUBLE(10,8), longitude DOUBLE(10,8), PRIMARY KEY (ID) );")
          line = 0;
          for attr in file_reader:
              if line == 0:
                  line = 1
              else:
                  data = []
                  data.extend((attr[0], attr[1], attr[2],attr[3], attr[4], attr[5], attr[6], attr[7], attr[8], attr[9], attr[10], attr[11], attr[12], attr[13]))
                  dataPeople.append(data)
          #cursor.executemany("""INSERT INTO earthquake_table (time, latitude, longitude, depth, mag, magType, nst, gap, dmin, rms, net, id, updated, place, type, horizontalError, depthError, magError, magNst, status, locationSource, magSource) VALUES(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);""", [(millisec1, attr[1], attr[2],attr[3], attr[4], attr[5], attr[6], attr[7], attr[8], attr[9], attr[10], attr[11], millisec2, attr[13], attr[14], attr[15], attr[16], attr[17], attr[18], attr[19], attr[20], attr[21])])
          stmt = """INSERT INTO people_table (gender, givenName, surName, streetAddress, city, state, emailAddress, username, telephoneNumber, Age, bloodType, centimeters, latitude, longitude) VALUES(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);"""
          cursor.executemany(stmt, dataPeople)
          conn.commit()
          cursor.close()
          conn.close()
          time_end = datetime.now()
          time_diff = time_end - time_start
  return render_template('complete.html', time_diff=time_diff)


if __name__ == '__main__':
    app.secret_key = 'mysecretkey'
    app.run(debug=True)
app.secret_key = 'secretkey'
