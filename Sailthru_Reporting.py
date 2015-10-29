__author__ = 'brucepannaman'

from sailthru.sailthru_client import SailthruClient
from sailthru.sailthru_response import SailthruResponseError
from sailthru.sailthru_error import SailthruClientError
import datetime
import psycopg2
import csv
import tinys3
import os

api_key = '***'

api_secret = '***'

sailthru_client = SailthruClient(api_key, api_secret)

# Validate if the connection to Sailthru works
try:
    response = sailthru_client.api_get("user", {"id": "bruce@pannaman.com"})
except SailthruClientError:
    print SailthruResponseError

# Log if the connection is live
if response.is_ok():
    body = response.get_body()
    print "Test went ok"

# This is the final array of each day that will be written into the db
output_array = []

day = datetime.timedelta(days=1)

#Set the date range of the array
start_date = datetime.date(2015, 1, 01)
# end_date = datetime.date(2015, 1, 30)
end_date = datetime.date.today()

# For each day go through response
while start_date <= end_date:
    date_iq = start_date.strftime("%Y-%m-%d")

    response2 = sailthru_client.api_get("stats",
                                        {"stat": "blast", "start_date": "%s" % date_iq, "end_date": "%s" % date_iq})

    response = response2.get_body()
    date_list = [date_iq]

# Validate json based on the length of the body, if there are no purchases there will be 14 key value pairs,
# if there are two then we most likely didn't send anything that day

    json_length = len(response)
    print json_length

    if json_length >= 4:
        try:
            response["purchase"]

        except:
            response["purchase"] = 0
        try:
            response["rev"]

        except:
            response["rev"] = 0
        try:
            response["purchase_price"]

        except:
            response["purchase_price"] = 0
        try:
            response["purchase_first"]

        except:
            response["purchase_first"] = 0
        try:
            response["purchase_second"]

        except:
            response["purchase_second"] = 0
        try:
            response["count"]

        except:
            response["count"] = 0
        try:
            response["pv"]

        except:
            response["pv"] = 0

        try:
            response["open_total"]

        except:
            response["open_total"] = 0

        try:
            response["estopens"]

        except:
            response["estopens"] = 0
        try:
            response["optout"]

        except:
            response["optout"] = 0
        try:
            response["softbounce"]

        except:
            response["softbounce"] = 0
        try:
            response["spam"]

        except:
            response["spam"] = 0
        try:
            response["click"]

        except:
            response["click"] = 0
        try:
            response["beacon"]

        except:
            response["beacon"] = 0
        try:
            response["click_multiple_urls"]

        except:
            response["click_multiple_urls"] = 0
        try:
            response["click_total"]

        except:
            response["click_total"] = 0
        try:
            response["hardbounce"]

        except:
            response["hardbounce"] = 0
        try:
            response["confirmed_opens"]

        except:
            response["confirmed_opens"] = 0
        try:
            response["view"]

        except:
            response["view"] = 0


        body2 =  date_list + response.values()
        print body2
        output_array.append(body2)


    if json_length < 4:
        print "Json_length < 4"
        response = {"count": 0, "purchase": 0, "pv": 0, "open_total": 0, "estopens": 0, "optout": 0,
                "softbounce": 0, "spam": 0, "rev": 0, "purchase_second": 0, "hardbounce": 0,
                "click": 0, "beacon": 0, "click_multiple_urls": 0, "purchase_first": 0,
                "click_total": 0, "purchase_price": 0, "confirmed_opens": 0, "view": 0}

        body2 =  date_list + response.values()
        print body2
        output_array.append(body2)
    else:
        print "Another day another lot of emails"

    print body2
    start_date = start_date + day



# Stick the results in a csv file

print "Writing all daily Sailthru data to CSV"
with open('Sailthru_report.csv', 'wb') as csvfile:
    spamwriter = csv.writer(csvfile, delimiter=',', quotechar='"')
    for rows in output_array:
        spamwriter.writerow(rows)
        print rows

# Shang it up to s3
print "Sending the CSV file to s3"
conn = tinys3.Connection('{AWS KEY}','{AWS SECRET}',endpoint='s3-eu-west-1.amazonaws.com')
f = open('Sailthru_report.csv','rb')
conn.upload('Sailthru_report.csv',f,'busuu.data/Sailthru')

print "Deleting local copy of CSV"
os.remove('Sailthru_report.csv')


# Connect to RedShift
conn_string = "dbname='***' port='5439' user='busuukm' password='***' host='***'"
print "Connecting to database\n        ->%s" % (conn_string)
conn = psycopg2.connect(conn_string)

cursor = conn.cursor()

# Update the redshift table with the new results

print "Creating new table \n sailthru_daily_digest2 "
cursor.execute("CREATE TABLE sailthru_daily_digest2(report_date date,count int, purchase int,click_total int, open_total int, optout int, softbounce int, spam int, rev int,purchase_second int,click_multiple_urls int, hardbounce int, purchase_price int,beacon int, estopens int, purchase_first int,pv int, click int, confirmed_opens int, view int);")
print "Copying S3 version of Sailthru CSV data to  \n sailthru_daily_digest2 "
cursor.execute("COPY sailthru_daily_digest2  FROM 's3://busuu.data/Sailthru/Sailthru_report.csv'  CREDENTIALS 'aws_access_key_id={AWS KEY};aws_secret_access_key={AWS SECRET}' delimiter ',';")
print "Dropping Table  \n sailthru_daily_digest "
cursor.execute("DROP TABLE sailthru_daily_digest;")
print "Renaming table  \n sailthru_daily_digest2 \nto \n sailthru_daily_digest "
cursor.execute("ALTER TABLE sailthru_daily_digest2 RENAME TO sailthru_daily_digest")


conn.commit()
conn.close()
