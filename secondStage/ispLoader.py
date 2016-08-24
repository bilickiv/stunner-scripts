# -*- coding: utf-8 -*-
# The goal of this csrip is to generate user specific data files

import sys
import json
import os
from datetime import datetime
import pymysql.cursors
import requests
import time


reload(sys)  
sys.setdefaultencoding('utf8')
config = {
'user': 'root',
'password': '',
'host': '10.6.14.37',
'database': 'stunner'
}


def saveToDb(ipAddress,data, cnx):
    continent_code = data['geolocation_data']['continent_code']
    continent_name = data['geolocation_data']['continent_name']
    country_code_iso3166alpha2 = data['geolocation_data']['country_code_iso3166alpha2']
    country_code_iso3166alpha3 = data['geolocation_data']['country_code_iso3166alpha3']
    country_code_iso3166numeric = data['geolocation_data']['country_code_iso3166numeric']
    country_code_fips10 = data['geolocation_data']['country_code_fips10-4']
    country_name = data['geolocation_data']['country_name']
    region_code = data['geolocation_data']['region_code']
    region_name = data['geolocation_data']['region_name']
    city = data['geolocation_data']['city']
    postal_code = data['geolocation_data']['postal_code']
    metro_code = data['geolocation_data']['metro_code']
    area_code = data['geolocation_data']['area_code']
    latitude = data['geolocation_data']['latitude']
    longitude = data['geolocation_data']['longitude']
    isp = data['geolocation_data']['isp']
    organization = data['geolocation_data']['organization']
    query = ("INSERT INTO IP2ISPDATA"
             "( ipAddress,continent_code,continent_name,country_code_iso3166alpha2, country_code_iso3166alpha3, country_code_iso3166numeric, country_code_fips10, country_name, region_code,region_name,city,postal_code,metro_code,area_code,latitude,longitude,isp,organization)"
             "VALUES(%s,%s,%s,%s, %s, %s, %s, %s, %s,%s,%s,%s,%s,%s,%s,%s,%s,%s)")
    ispData = (ipAddress,continent_code,continent_name,country_code_iso3166alpha2, country_code_iso3166alpha3, country_code_iso3166numeric, country_code_fips10, country_name, region_code,region_name,city,postal_code,metro_code,area_code,latitude,longitude,isp,organization)             
    cursor = cnx.cursor()
    cursor.execute( query, ispData )
    cnx.commit() 
    cursor.close()
    return

def getMetainfoBasedOnIPs():
    cnx = pymysql.connect(**config)
    cursor = cnx.cursor()
    query = "select  DISTINCT publicIP from TMPIP2DATA WHERE publicIP  NOT IN (SELECT DISTINCT ipAddress from IP2ISPDATA) order by publicIP"#" where hashId like '%gqVdRgqBInW1CcUXXi%'"
    ret = cursor.execute( query )
    results = cursor.fetchall()
    index1 = 0
    print "Starting to save total of " + str(len(results)) + " files"
    for row in results:
        time.sleep(1.1)
        index1 = index1 + 1
        print "//////////////////////////////////////////////////////////////////////////////"
        print str(index1) + ". the IP: " + str(row[0]) 
        resp = requests.get('http://api.eurekapi.com/iplocation/v1.8/locateip?key=SAKRY6E2G945EZ492VNZ&ip='+str(row[0])+'&format=JSON')
        if resp.status_code != 200:
             # This means something went wrong.
            raise ApiError('GET /tasks/ {}'.format(resp.status_code))
        #print resp.json()
        data = resp.json()
        if(data['query_status']['query_status_description'] == 'Query successfully performed.'):
            saveToDb(str(row[0]),data, cnx)
        else:
            print data['query_status']
            
    cnx.commit()
    return
      
import glob
#path = "*.imp"
cnx = pymysql.connect(**config)
#for fname in glob.glob(path):print("Loading file:" + fname + "----" + unicode(datetime.datetime.now()))
 #   uploadFile(fname)
getMetainfoBasedOnIPs()
cnx.commit() 
cnx.close()
          

              
#loadFile("../402036")      
      