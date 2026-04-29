import os
import sys

import boto3
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

from pathlib import Path
from datetime import datetime
from io import BytesIO


BUCKET_NAME = 'itz-s3-eastus2-bronze-001'
REGION = 'us-east-2'
DOWNLOADS_PATH = Path.home() / 'Downloads' / 'students.csv'

def get_input_data():
    print(f'Scaning the route: {DOWNLOADS_PATH}')
    if not DOWNLOADS_PATH.exists():
        print(f'Error: The `{DOWNLOADS_PATH}` path was not find!')
        print(f'    Check if the the file is in Downloads')
        return None
    
    print(f'File finded in: {DOWNLOADS_PATH}')
    size_kb = DOWNLOADS_PATH.stat().st_size / 1024 
    print(f'    Size file: {size_kb:.2f} KB')
    return DOWNLOADS_PATH


def parquet_formater(csv_path):
    try:
        df = pd.read_csv(csv_path)
        rows = len(df)

        print(f'CSV loaded')
        print(f'Total rows: {rows:,}')
        print(f'Headers: {",".join(df.columns.tolist())}')
        
        df = df.convert_dtypes()
        print(f'/n First 3 rows:')
        print(df.head(3).to_string(index=False))

        print(f'/nChange into the new format')
        parquet_buffer = BytesIO()
        df.to_parquet(
            parquet_buffer, 
            engine='pyarrow',
            compression='snappy',
            index=False
        )
        parquet_buffer.seek(0)
        csv_size = csv_path.stat().st_size / 1024
        parquet_size = len(parquet_buffer.getvalue()) / 1024
        compression = (1 - parquet_size / csv_size) * 100  if (csv_size > 0) else 0 

        print(f'Conversion completed')
        print(f'   csv size: {csv_size:.2f} KB')
        print(f'   parquet size: {parquet_size:.2f} KB')
        print(f'   compression: {compression:.1f} %')

        return parquet_buffer, rows
        
    except Exception as e:
        print(f'Error when trying the conversion: {str(e)}')
        return None, 0
    

def file_uploader(parquet_buffer, row_number):
    print(f'\nUploading to S3...')
    try:
        s3_client = boto3.client('s3', region_name=REGION)

        now = datetime.now()
        year = now.strftime('%Y')
        month = now.strftime('%m')
        day = now.strftime('%d')
        timestamp = now.strftime('%Y%m%d %H:%M:%S')

        s3_key = f'raw/students/year={year}/month={month}/day={day}/students_{timestamp}.parquet'
        print(f'Destiny: s3://{BUCKET_NAME}/{s3_key}')
        s3_client.put_object(
            Bucket=BUCKET_NAME,
            Key=s3_key,
            Body=parquet_buffer.getvalue(),
            ContentType='application/octet-stream',
            StorageClass='STANDARD',
            Metadata={
                'source': 'downloads',
                'records': str(row_number),
                'ingestion_date': now.isoformat(),
                'format': 'parquet',
                'compression': 'snappy'
            }
        )

        print(f'Filed was uploaded sucessfully!')

        # validation
        response = s3_client.head_object(
            Bucket=BUCKET_NAME, 
            Key=s3_key
        )

        print(f'Validation')
        print(f'   S3 size: {response["ContentLength"] / 1024:.2f} KB')
        print(f'   Storage class: {response.get("StorageClass", "STANDARD")}')
        print(f'   eTag: {response["ETag"]}')

        return s3_key

    except Exception as e:
        print(f'Error trying to upload the file: {str(e)}')


def clean_bucket(days: int = 7):
    print(f"/nRemoving files older than {days} days)")

    try:
        s3_client = boto3.client('s3', region_name=REGION)
        
        response = s3_client.list_objects_v2(
            Bucket=BUCKET_NAME,
            Prefix='raw/students/'
        )

        if 'Contents' not in response:
            print('    There are no old files to remove')
            return
        
        now = datetime.now()
        deleted_count = 0

        for object in response['Contents']:
            age_days = (now - object['lastModified'].replace(tzinfo=None)).days

            if age_days > days:
                s3_client.deleter_object(
                    Bucket=BUCKET_NAME,
                    Key=object["Key"]
                )
                deleted_count += 1
                print(f'File delited: {object["Key"]} with {age_days}')
                
            if deleted_count == 0:
                print(f'No hay archivos antiguos')
            else:
                print(f'{deleted_count} files were deleted.')

    except Exception as e:
        print(f'Error trying to remove older files{str(e)}')


def main():
    print("==START ETL ORCHESTRATION==")
    print(f"Process date: {datetime.now().strftime('%Y%m%d %H:%M:%S')}")
    print(f"Bucket target: {BUCKET_NAME}")
    print(f"Region: {REGION}")

    csv_path = get_input_data()
    if not csv_path:
        sys.exit(1)

    parquet_buffer, row_number = parquet_formater(csv_path)
    if not parquet_buffer:
        sys.exit(1)

    s3_key = file_uploader(parquet_buffer, row_number)
    if not s3_key:
        sys.exit(1)

    clean_bucket()

    print('==ETL ORCHESTRATION COMPLETED==')
    print(f'''
        Result:
            - Procesed rows: {row_number:,}
            - Originin file: {csv_path}
            - Destinity file: s3://{BUCKET_NAME}/{s3_key}
        
    ''')


if __name__ == "__main__":
    main()