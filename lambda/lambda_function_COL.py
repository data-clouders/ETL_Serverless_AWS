import json
import boto3
import urllib3
from datetime import datetime

# Initialize S3 client
s3 = boto3.client('s3')

# Replace with your S3 bucket name
S3_BUCKET_NAME = 's3_raw_data'
S3_FOLDER_NAME = "COL/"

def lambda_handler(event, context):
    # API URL
    url = "https://www.datos.gov.co/resource/gt2j-8ykr.json"
    http = urllib3.PoolManager()
    
    try:
        # Send GET request
        response = http.request('GET', url)

        
        
        # Check for successful response
        if response.status == 200:
            data = json.loads(response.data.decode('utf-8'))
            # Log the data (or process it as needed)
            # Generate a filename with timestamp
            timestamp = datetime.now().strftime('%Y-%m-%d_%H-%M-%S')
            file_name = f"datos_response_{timestamp}.json"
            
            # Save data to S3
            s3.put_object(
                Bucket=S3_BUCKET_NAME,
                Key=f"{S3_FOLDER_NAME}{file_name}",
                Body=json.dumps(data),
                ContentType='application/json'
            )
            
            # Return the data
            return {
                'statusCode': 200
            }
        else:
            return {
                'statusCode': response.status,
                'body': f"Failed to fetch data: {response.data.decode('utf-8')}"
            }
    except Exception as e:
        # Handle exceptions
        return {
            'statusCode': 500,
            'body': f"Error: {str(e)}"
        }