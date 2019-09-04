import os
import configparser
import boto3
import psycopg2
import json
import re
import datetime
import pandas as pd
from pandas.io.json import json_normalize
from sodapy import Socrata
from sql_queries import load_ndc_queries


def format_ndc(ndc):
    '''Convert NDC code to 11 digits per www.drugs.com/ndc.html'''
    # 4-4-2
    if re.match('^[0-9]{4}-[0-9]{4}-[0-9]{2}$', ndc):
        return '0' + ndc.replace('-','')
    # 5-3-2
    elif re.match('^[0-9]{5}-[0-9]{3}-[0-9]{2}$', ndc):
        return ndc[0:5] + '0' + ndc[6:9] + ndc[10:]
    # 5-4-1
    elif re.match('^[0-9]{5}-[0-9]{4}-[0-9]{1}$', ndc):
        return ndc.replace('-','')[0:9] + '0' + ndc[-1] 
    else: 
        return None


def copy_from_S3_to_S3(s3, source, destination, key):
    '''Copy files between S3 buckets.
    
    s3: s3 resource
    source: source bucket
    destination: destination bucket
    key: prefix for source and destination
    
    '''
    for obj in s3.Bucket(source).objects.filter(Prefix=key):  
        print('Copying ' + obj.key)
        copy_source = {
            'Bucket': source,
            'Key': obj.key
        }
        s3.meta.client.copy(copy_source, destination, obj.key)
        

def process_labels(s3, source, destination, key):
    '''Download labels JSONs, process data and save to S3 in csv format.
    
    s3: s3 resource
    source: source bucket
    destination: destination bucket
    key: prefix for source and destination
    
    '''
    for obj in s3.Bucket(source).objects.filter(Prefix=key):  
        # download file
        fn=obj.key.split('/')[-1]
        s3.Object(source, obj.key).download_file(Filename=fn)
        print('processing ' + obj.key)

        # load to dict
        with open(fn) as f:
            labels_dict = json.load(f)
        
        # normalize
        labels = json_normalize(labels_dict['results'])
        
        # convert invalid column names
        labels = labels.rename({
                'openfda.package_ndc':'package_ndc',
                'openfda.generic_name':'generic_name',
                'openfda.brand_name':'brand_name',
                }, axis=1)

        # select columns
        desired_columns = ['active_ingredient',
                           'adverse_reactions',
                           'brand_name',
                           'drug_interactions',
                           'generic_name',
                           'indications_and_usage', 
                           'package_ndc',
                           'warnings']

        labels = labels[desired_columns]
        
        # explode ndc's
        labels = labels.explode('package_ndc')
        
        # format ndc's
        labels['formatted_ndc'] = [format_ndc(str(x)) for x in
                                            labels['package_ndc']]
        
        # sort columns to match database table
        labels = labels[sorted(labels.columns)]
        
        # save to destination
        outfile = os.path.join('s3a://', destination, obj.key[0:-5], '.csv')
    
        labels.to_csv(outfile, sep='|', header=0, index=0)
        print('processed and saved as csv')

        # delete file
        os.remove(fn)
        

def process_drug_events(s3, source, destination, key):
    '''Download drug events JSONs, process data and save to S3 as csv.
    
    s3: s3 resource
    source: source bucket
    destination: destination bucket
    key: prefix for source and destination
    
    '''
    for obj in s3.Bucket(source).objects.filter(Prefix=key):  
        
        # download file
        fn=obj.key.split('/')[-1]
        s3.Object(source, obj.key).download_file(Filename=fn)
        
        print('processing ' + obj.key)

        # load to dict
        with open(fn) as f:
            events_dict = json.load(f)
        
        # normalize nested patient-drug data
        drugs = json_normalize(events_dict['results'], 
                               ['patient', 'drug'], 
                               'safetyreportid')
        
        # skip file if package_ndc element is missing
        if not 'openfda.package_ndc' in drugs.columns.values:
            print('package_ndc missing from ' + fn)
            os.remove(fn)
            continue
        
        # correct invalid column names
        drugs = drugs.rename({'openfda.package_ndc':'package_ndc'}, axis=1)
            
        # create drug_events fact table
        drug_events = (drugs[['package_ndc', 'safetyreportid']]
                      .explode('package_ndc')
                      .drop_duplicates())

        # convert 10-digit ndc to 11-digit format
        drug_events['formatted_ndc'] = [format_ndc(str(x)) for x in
                                            drug_events['package_ndc']]
        
        # sort columns to match database table
        drug_events = drug_events[sorted(drug_events.columns)]
        
        # drop row if data missing
        drug_events = drug_events.dropna()
        
        # normalize nested patient reactions data
        reactions = json_normalize(events_dict['results'], 
                                  ['patient', 'reaction'], 
                                  'safetyreportid')
        
        reactions_columns = ['reactionmeddrapt', 'safetyreportid']
        
        reactions = reactions[reactions_columns]
        
        # normalize safetyreports data
        sr = json_normalize(events_dict['results'], max_level=1)

        desired_columns = ['safetyreportid', 'receivedate', 'receiptdate',
                           'seriousnesshospitalization','seriousnessdeath',
                           'seriousnesslifethreatening','seriousnessdisabling',
                           'seriousnesscongenitalanomali',
                           'patient.patientdeath']

        available_columns = [colname for colname in sr.columns.values 
                             if colname in desired_columns]

        safetyreports = pd.DataFrame(columns=['safetyreportid', 
                                       'receivedate', 
                                       'receiptdate', 
                                       'seriousnesshospitalization',
                                       'seriousnessdeath',
                                       'seriousnesslifethreatening',
                                       'seriousnessdisabling',
                                       'seriousnesscongenitalanomali',
                                       'patient.patientdeath'])

        safetyreports = safetyreports.append(sr[available_columns], sort=True)

        safetyreports.rename({'patient.patientdeath':'patientdeath'}, 
                             axis=1, inplace=True)
         
        safetyreports['receiptdate'] = safetyreports['receiptdate'].apply(
                lambda x: pd.to_datetime(x))
        
        safetyreports['receivedate'] = safetyreports['receivedate'].apply(
                lambda x: pd.to_datetime(x)) 

        safetyreports = (safetyreports[sorted(safetyreports.columns)]
                        .drop_duplicates())
            
        # save to destination
        safetyreports_key = 'safetyreports/' + obj.key[0:-5]
        
        safetyreports_outfile = os.path.join('s3a://', destination,
                                             safetyreports_key)
    
        safetyreports.to_csv(safetyreports_outfile, sep='|', header=0, index=0)
        
        drug_events_key = 'drug_events/' + obj.key[0:-5]
        
        drug_events_outfile = os.path.join('s3a://', destination,
                                           drug_events_key)
        
        drug_events.to_csv(drug_events_outfile, sep='|', header=0, index=0)
        
        reactions_key = 'reactions/reactions_' + obj.key[0:-5]
        
        reactions_outfile = os.path.join('s3a://', destination, reactions_key)
        
        # skip reactions if 'reactionmeddrapt' column missing 
        if 'reactionmeddrapt' in reactions.columns.values:
            reactions.to_csv(reactions_outfile, sep='|', header=0, index=0)
        
        print('processed and saved as csv')
        
        # delete file
        os.remove(fn)
        
def get_pricing_data(app_token, destination, key, first=1):
    '''Download NADAC pricing, process data and save to S3 as csv.
    
    app_token: Socrata app token
    destination: destination bucket
    key: prefix for destination
    
    '''
    # download pricing data
    client = Socrata('data.medicaid.gov', app_token, timeout=100)

    # download all data on first data load
    if first==1:

        # get record count
        q = '''select count(as_of_date) as row_count '''
        
        row_count = int(client.get("tau9-gfwr", query=q)[0]['row_count'])
    
        # page through data to download
        offset = -100000
        limit = 100000
        while offset < row_count:
            offset += limit
            #returned as JSON by API > converted to list of dicts by sodapy 
            results = client.get("tau9-gfwr", limit=limit, offset=offset)
            if offset == 0:
                pricing = pd.DataFrame.from_records(results)
            else:
                pricing = pricing.append(pd.DataFrame.from_records(results),
                                         sort=True)
    else:
        # weekly updates: download latest as_of_date (about 25k rows)
         q2 = '''select max(as_of_date) as max_as_of_date '''
         
         max_as_of_date = client.get("tau9-gfwr", query=q2)[0]['max_as_of_date']
         
         q3 = '''select * where as_of_date = {}'''.format(max_as_of_date)
         
         results = client.get("tau9-gfwr", query=q3)
         
         pricing = pd.DataFrame.from_records(results)
             
    client.close()
    
    # wrangle dates
    pricing['as_of_date'] = (pricing['as_of_date']
            .apply(lambda x: pd.Timestamp(x)))
    
    pricing['effective_date'] = (pricing['effective_date']
            .apply(lambda x: pd.Timestamp(x)))
    
    pricing['corresponding_generic_drug_effective_date'] = (
            pricing['corresponding_generic_drug_effective_date']
            .apply(lambda x: pd.Timestamp(x)))
    
    # save to destination
    outfile = os.path.join('s3a://', destination, 'pricing/pricing')
    
    pricing.to_csv(outfile, sep='|', header=0, index=0)
    
    print(str(len(pricing)) + ' pricing records processed and saved as \
          csv')


def load_data_into_redshift(key, table, s3, source, cur, ARN):
    '''Copy data from csv files on S3 to table in redshift database.
    
    s3: S3 resource
    source: S3 source bucket
    key: S3 prefix
    table: destination table
    cur: psycopg2 cursor
    
    '''
    for obj in s3.Bucket(source).objects.filter(Prefix=key):

        s3_location = str(os.path.join('s3://', source, obj.key))

        copy_query = ("""COPY {} FROM '{}' iam_role '{}' CSV DELIMITER '|' TRUNCATECOLUMNS;""").format(table, s3_location, ARN)

        cur.execute(copy_query)
        
        print(str(obj.key) + ' loaded to table ' + table)
        
        
def load_ndc_table(cur, conn):
    """Run load ndc table queries."""
    for query in load_ndc_queries:
        cur.execute(query)
        conn.commit()
        

def main():
    # load Socrata credentials
    config = configparser.ConfigParser()
    config.read('Socrata_credentials.cfg')
    app_token = config.get('Socrata', 'AppToken')

    # load AWS credentials 
    config.read('capstone.cfg')
    AWS_ACCESS_KEY_ID = config.get('AWS', 'KEY')
    AWS_SECRET_ACCESS_KEY = config.get('AWS', 'SECRET')
    ARN = config.get('IAM_ROLE', 'ARN')
    
    # initialize s3 resource
    s3 = boto3.resource('s3',
                        aws_access_key_id=AWS_ACCESS_KEY_ID,
                        aws_secret_access_key=AWS_SECRET_ACCESS_KEY)

    # set source and destination buckets
    openFDA_bucket = "download.open.fda.gov"
    zipped_bucket = "dend-rbmayer-zipped"
    unzipped_bucket = "dend-rbmayer-unzipped"
    labels_key = "drug/label" 
    drug_events_key = "drug/event"
    destination_bucket = "dend-rbmayer"

    # copy labels archives from openFDA to zipped bucket
      # AWS Lambda function will automatically unzip files
    copy_from_S3_to_S3(s3, openFDA_bucket, zipped_bucket, labels_key)
    
    # process labels
    process_labels(s3, unzipped_bucket, destination_bucket, labels_key)
    
    # copy drug events archives from openFDA to zipped bucket
    copy_from_S3_to_S3(s3, openFDA_bucket, zipped_bucket, drug_events_key)
    
    # process drug events
    process_drug_events(s3, unzipped_bucket, destination_bucket, 
                        drug_events_key)

    # process pricing data
    get_pricing_data(app_token, destination_bucket, 'pricing')

    # load data into redshift
    keys_and_tables = [['pricing', 'pricing'] ,
                       [labels_key, 'labels'],
                       ['safetyreports', 'safetyreports'],
                       ['reactions', 'reactions']#,
                       ['drug_events', 'drug_events']]
    
    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}"
                            .format(*config['CLUSTER'].values()))
    conn.autocommit = True
    cur = conn.cursor()
    
    for pair in keys_and_tables:
        load_data_into_redshift(*pair, s3, destination_bucket, cur, ARN)
        
    # populate ndc table
    load_ndc_table(cur, conn)
        
    conn.close()


if __name__ == "__main__":
    main()
