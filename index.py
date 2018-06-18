import configparser
import os

from time import time
from flask import Flask, request, render_template, url_for
import boto3


def log_image(client, domain, filename, processed, timestamp):
    response = client.put_attributes(
        DomainName=domain,
        ItemName='image',
        Attributes=[
            {
                'Name': 'image_name',
                'Value': filename,
                'Replace': True
            },
            {
                'Name': 'processed',
                'Value': processed,
                'Replace': True
            },
            {
                'Name': 'timestamp',
                'Value': str(timestamp),
                'Replace': True
            },
        ],
    )
    return response


app = Flask(__name__)

AWS_ACCESS_KEY_ID = os.environ['AWS_ACCESS_KEY_ID']
AWS_SECRET_ACCESS_KEY = os.environ['AWS_SECRET_ACCESS_KEY']

SIMPLE_DB_DOMAIN_NAME = os.environ['SIMPLE_DB_DOMAIN_NAME']

QUEUE_NAME = os.environ['QUEUE_NAME']
BUCKET_NAME = os.environ['BUCKET_NAME']
BUCKET_URL = 'https://s3-us-west-2.amazonaws.com/{}'.format(BUCKET_NAME)

s3 = boto3.resource('s3', aws_access_key_id=AWS_ACCESS_KEY_ID,
                    aws_secret_access_key=AWS_SECRET_ACCESS_KEY)
sqs = boto3.resource('sqs', region_name='us-west-2', aws_access_key_id=AWS_ACCESS_KEY_ID,
                     aws_secret_access_key=AWS_SECRET_ACCESS_KEY)
queue = sqs.get_queue_by_name(QueueName=QUEUE_NAME)

simple_db_client = boto3.client('sdb', region_name='us-west-2', aws_access_key_id=AWS_ACCESS_KEY_ID,
                                aws_secret_access_key=AWS_SECRET_ACCESS_KEY)


@app.route("/")
def index():
    return render_template('index.html')


@app.route('/upload', methods=['GET'])
def upload():
    return '<h1>File saved to S3</h1>'


@app.route('/success')
def successfull_upload():
    return "<h1>File saved to S3</h1> <a href='/images'>Images</a>"


def get_url(filename):
    return '{}/{}'.format(BUCKET_URL, filename)


@app.route('/images', methods=['GET', 'POST'])
def images():
    bucket_files = s3.Bucket(BUCKET_NAME).objects.filter(Prefix='uploads/')
    edge_files = s3.Bucket(BUCKET_NAME).objects.filter(Prefix='edges/')

    uploaded_files = [file.key for file in bucket_files]
    processed_files = [file.key for file in edge_files]

    table_content = []
    for file in uploaded_files:
        filename = file.split('/')[-1]
        if "edges/" + filename in processed_files:
            table_content.append(
                [file, get_url(file), "edges/" + filename, get_url("edges/" + filename)])
        else:
            table_content.append([file, get_url(file), ""])

    return render_template('images.html', uploaded_files=uploaded_files, table_content=table_content)


def get_selected_images(request):
    items = request.form.getlist('imagesSelection')
    for filename in items:
        log_image(simple_db_client, SIMPLE_DB_DOMAIN_NAME,
                  filename, 'False', time())
        queue.send_message(MessageBody=filename)
    return items


@app.route('/process-selected', methods=['GET', 'POST'])
def selected_images():
    items = get_selected_images(request)
    return "<h1>Images submited</h1><a href='images'>Images</a>"


if __name__ == "__main__":
    app.run(host='0.0.0.0', port=80)
