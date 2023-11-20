from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta

# Define the generator function for date combinations
def generate_date_combinations(start_date, end_date):
    current_date = start_date
    while current_date <= end_date:
        yield current_date.strftime("%Y/%m/%d/%H")
        current_date += timedelta(hours=1)

# Define a function to process each date combination
def process_date_combination(date_combination):
    # Dummy function to simulate processing
    # Replace with actual processing logic
    print(f"Processing {date_combination}")

# Define a function to set up threading and process date combinations
def generate_and_process_dates(start_date, end_date, max_workers=5):
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        # Create a future for each date combination
        futures = [executor.submit(process_date_combination, dc) for dc in generate_date_combinations(start_date, end_date)]
        
        # Wait for all the futures to complete
        for future in futures:
            future.result()  # This will block until the future is complete

# Main execution
if __name__ == "__main__":
    # Define the start and end dates
    start_date = datetime(2023, 3, 1, 0, 0)
    end_date = datetime(2024, 3, 31, 23, 59)

    # Process the date combinations using multithreading
    generate_and_process_dates(start_date, end_date)




###########################################################################

import boto3

# Initialize a session using Amazon S3 credentials
session = boto3.Session(
    aws_access_key_id='YOUR_ACCESS_KEY',
    aws_secret_access_key='YOUR_SECRET_KEY',
    region_name='YOUR_REGION'
)
s3 = session.resource('s3')

def process_date_combination(date_combination):
    source_bucket = s3.Bucket('your-source-bucket-name')
    target_bucket = 'your-target-bucket-name'
    prefix = f'your/source/prefix/{date_combination}'

    for obj in source_bucket.objects.filter(Prefix=prefix):
        copy_source = {
            'Bucket': 'your-source-bucket-name',
            'Key': obj.key
        }
        target_key = obj.key.replace('your/source/prefix/', 'your/target/prefix/')

        # Copy the object
        s3.meta.client.copy(copy_source, target_bucket, target_key)
        print(f"Copied {obj.key} to {target_key}")










###########################################################################################
###########################################################################################
###########################################################################################



from datetime import datetime, timedelta

# Updated generator function to handle custom start and end times
def generate_date_combinations(start_date, end_date, delta=timedelta(minutes=1)):
    """
    This generator function yields date combinations in the format 'YYYY/MM/DD/HH/MM'
    for each interval between start_date and end_date.
    If the timedelta is less than 1 hour, the start date is adjusted to the beginning of the previous hour.
    """
    # If the end date is within the same hour as the start date, adjust the start date to one hour before
    if start_date.hour == end_date.hour and start_date.date() == end_date.date():
        start_date = start_date.replace(minute=0, second=0) - timedelta(hours=1)

    current_date = start_date
    while current_date <= end_date:
        yield current_date.strftime("%Y/%m/%d/%H/%M")
        current_date += delta

# Define the start and end dates with less than an hour difference
start_date = datetime(2023, 3, 1, 11, 0)
end_date = datetime(2023, 3, 1, 11, 30)

# Example usage of the generator with a specific timedelta
for date_combination in generate_date_combinations(start_date, end_date, timedelta(minutes=1)):
    print(date_combination)


###########################################################################################
###########################################################################################
###########################################################################################







####################################################################################################
####################################################################################################
####################################################################################################



import subprocess
import os
from concurrent.futures import ThreadPoolExecutor

def aws_cli_copy(source_bucket, destination_bucket, aws_profile='default'):
    """
    Copy files from one S3 bucket to another using the AWS CLI.

    :param source_bucket: The source S3 bucket path (e.g., s3://my-source-bucket/path/)
    :param destination_bucket: The destination S3 bucket path (e.g., s3://my-destination-bucket/path/)
    :param aws_profile: The AWS CLI profile name to use for credentials
    """
    try:
        # Set up the AWS profile (if not default)
        os.environ['AWS_PROFILE'] = aws_profile

        # Run the AWS CLI command to sync the buckets
        subprocess.run(['aws', 's3', 'sync', source_bucket, destination_bucket], check=True)
        print(f"Files copied from {source_bucket} to {destination_bucket}")
    except subprocess.CalledProcessError as e:
        print(f"An error occurred while copying: {e}")
    except Exception as e:
        print(f"An unexpected error occurred: {e}")

# Function to execute the copy tasks using ThreadPoolExecutor
def execute_aws_cli_copies(copy_tasks, max_workers=5):
    """
    Execute multiple AWS CLI copy operations concurrently.

    :param copy_tasks: A list of tuples, each containing arguments for the aws_cli_copy function
    :param max_workers: The maximum number of threads to use
    """
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        future_to_copy = {executor.submit(aws_cli_copy, *task): task for task in copy_tasks}
        for future in concurrent.futures.as_completed(future_to_copy):
            task = future_to_copy[future]
            try:
                future.result()
            except Exception as e:
                print(f"Task {task} generated an exception: {e}")

# Example usage:
# Define your copy tasks as a list of tuples
copy_tasks = [
    ('s3://my-source-bucket1/path/', 's3://my-destination-bucket1/path/', 'my-profile'),
    ('s3://my-source-bucket2/path/', 's3://my-destination-bucket2/path/', 'my-profile'),
    # Add more tasks as needed
]

# Execute the


######################################################################################################
######################################################################################################
######################################################################################################
