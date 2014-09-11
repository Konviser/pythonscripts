import urllib2
import httplib
import json
from datetime import datetime
import sqlite3

#from settings_loaddata import DB_LOCATION
#from settings_loaddata import DB_QUERY

URL="http://www.quandl.com/api/v1/datasets/QUANDL/USDEUR.json"


class APIHandler(object):

    def __init__(self,url):
        self.url = url
        self.errors = ""
        self.fp = None

        try:
            urlrequest = urllib2.Request(self.url)
            opener = urllib2.build_opener()
            self.fp = opener.open(urlrequest) #in some cases can return None

        except urllib2.HTTPError as httperr:
            self.errors = "You got an error with HTTP request: {0}".format(httperr)

        except urllib2.URLError as urlerr:
            self.errors = "You got an error with URL: {0}".format(urlerr)



class JsonLoader(object):

    def __init__(self, fp):
        self.fp = fp
        self.errors = ""
        self.jsondata = json.load(self.fp) #Throws AttributeError if self.fp is None



class JsonExchangeRate(JsonLoader):

    def __init__(self,fp):

        super(JsonExchangeRate,self).__init__(fp)
        self.jsondata = self.jsondata['data']

    def get_data(self):
        for row in self.jsondata:
            row[0] = datetime.strptime(row[0], "%Y-%m-%d")
            yield row


def insert_to_db(db, dbquery, data):

    try:
        with sqlite3.connect(db) as conn:
            cursor = conn.cursor()
            for row in data:
                cursor.execute(dbquery, row)
                conn.commit()

    except sqlite3.OperationalError as oe:
        print  "The exception was raised: {0}".format(oe)

    except sqlite3.Error as sqliterr:
            print 'You got problems with sqlite. The exception was raised: {0}'.format(sqliterr)
            conn.close()


def do_job():

    try:

        handler = APIHandler(URL)
        data_generator = JsonExchangeRate(handler.fp).get_data()

        #now we can insert data to db or do whatever we wand with them
        #insert_to_db(DB_LOCATION, DB_QUERY, data_generator)

    except AttributeError:
        print "Hmm looks like you've got problems with APIhandler: {0}".format(handler.errors)

    except KeyError:
        print "Hmm looks like json data does not contain: {0} attribute".format("data")

    except Exception as e:
            print 'You got an exeption: {0}'.format(e)


if __name__ == '__main__':

    do_job()
