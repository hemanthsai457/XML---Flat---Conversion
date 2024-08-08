import boto3
import xml.etree.ElementTree as ET
import csv
import os

s3 = boto3.client('s3')

def lambda_handler(event, context):
    bucket_name = event['Records'][0]['s3']['bucket']['name']
    object_key = event['Records'][0]['s3']['object']['key']
    
    # Get the XML file from S3
    xml_file = s3.get_object(Bucket=bucket_name, Key=object_key)['Body'].read().decode('utf-8')
    
    # Parse the XML
    root = ET.fromstring(xml_file)
    
    # Print XML tree structure for debugging
    for elem in root.iter():
        print(f"Tag: {elem.tag}, Text: {elem.text}")

    # Prepare data for CSV
    csv_data = []
    
    # Iterate over each 'row' element in the XML
    for row in root.findall('.//row'):
        row_data = {
            'unique_id': row.find('unique_id').text if row.find('unique_id') is not None else '',
            'indicator_id': row.find('indicator_id').text if row.find('indicator_id') is not None else '',
            'name': row.find('name').text if row.find('name') is not None else '',
            'measure': row.find('measure').text if row.find('measure') is not None else '',
            'measure_info': row.find('measure_info').text if row.find('measure_info') is not None else '',
            'geo_type_name': row.find('geo_type_name').text if row.find('geo_type_name') is not None else '',
            'geo_join_id': row.find('geo_join_id').text if row.find('geo_join_id') is not None else '',
            'geo_place_name': row.find('geo_place_name').text if row.find('geo_place_name') is not None else '',
            'time_period': row.find('time_period').text if row.find('time_period') is not None else '',
            'start_date': row.find('start_date').text if row.find('start_date') is not None else '',
            'data_value': row.find('data_value').text if row.find('data_value') is not None else '',
        }
        print(f"Row Data: {row_data}")
        csv_data.append(row_data)

    # Write to CSV file
    csv_file = '/tmp/converted.csv'
    if csv_data:
        with open(csv_file, mode='w', newline='') as file:
            writer = csv.DictWriter(file, fieldnames=csv_data[0].keys())
            writer.writeheader()
            writer.writerows(csv_data)
        
        # Upload CSV to S3
        s3.upload_file(csv_file, 'xml-flat-op', object_key.replace('.xml', '.csv'))
        return {'statusCode': 200, 'body': 'File converted successfully'}
    else:
        return {'statusCode': 200, 'body': 'No data found to convert'}
