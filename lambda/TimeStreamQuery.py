#
# TimestreamQuery
#

import base64
import boto3
import botocore
import json
import logging
import os
import json

logger = logging.getLogger()
logger.setLevel(logging.INFO)

DATABASE_SENSORDATA = os.environ.get("DATABASE_SENSORDATA", "")
TABLE_SENSORDATA = os.environ.get("TABLE_SENSORDATA", "").split('|')[-1]

logger.info("DATABASE_SENSORDATA: {} TABLE_SENSORDATA: {} ".format(DATABASE_SENSORDATA, TABLE_SENSORDATA))

QUERY =  '          (  '\
 '   SELECT measure_name, avg(measure_value::double) as average_temparature FROM ' + DATABASE_SENSORDATA + '.' + TABLE_SENSORDATA+ ' '\
 '   WHERE time > ago(120m)  '\
 '   AND measure_name=\'temperature\' '\
 '   GROUP BY measure_name  '\
 '   having avg(measure_value::double) > 20  '\
 '          )  '
 
def lambda_handler(event, context):
    logger.debug("event:\n{}".format(json.dumps(event, indent=2)))

    records_sensordata = []
    records_ggmetrics = []

    try:
        c_ts_query = boto3.client('timestream-query')
        sns = boto3.client('sns')

        response = c_ts_query.describe_endpoints()
        logger.info("response describe_endpoints: {}".format(response))
        if not DATABASE_SENSORDATA or not TABLE_SENSORDATA or len(DATABASE_SENSORDATA)==0 or len(TABLE_SENSORDATA)==0:
            logger.warn("database or table for sensordata not defined: DATABASE_SENSORDATA: {} TABLE_SENSORDATA: {}".format(DATABASE_SENSORDATA, TABLE_SENSORDATA))
            return {"status": "warn", "message": "database or table for sensordata not defined"}
        response = c_ts_query.query(QueryString=QUERY)
        if not response :
            return {"status": "error", "message": "Empty response"}
        ret_val = {"status": "success"}
        ret_val["records size"] = "{}" .format(len(response['Rows']))
        result = int(ret_val["records size"]);
    
    except Exception as e:
        logger.error("{}".format(e))
        return {"status": "query error", "message": "{}".format(e)}   

    try:
        if result < 1:
            return result

        if result>0 and 'temperature' in response['Rows'][0]['Data'][0]['ScalarValue']:
            sns.publish(TopicArn='arn:aws:sns:eu-west-1:<ACCOUNT_ID>:Alarm-threshold-trigger', Message="ACTION. REQUIRED!: Average Temparature theshold exceeded. Here is the average temparature value from last 2 mins:"+response['Rows'][0]['Data'][1]['ScalarValue'], Subject="Alert")
        
        
            
        return ret_val 

    except Exception as e:
        logger.error("{}".format(e))
        return {"status": "error sending notification", "message": "{}".format(e)}
        
     
