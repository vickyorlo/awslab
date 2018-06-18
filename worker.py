import boto3
import skimage.io
from skimage.color import rgb2gray
from skimage.filters import sobel
from skimage.util import invert
import os
from time import time


def log_image(client, domain, filename, processed, timestamp):
    response = client.put_attributes(
        DomainName=domain,
        ItemName=filename,
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


AWS_ACCESS_KEY_ID = os.environ['AWS_ACCESS_KEY_ID']
AWS_SECRET_ACCESS_KEY = os.environ['AWS_SECRET_ACCESS_KEY']

SIMPLE_DB_DOMAIN_NAME = os.environ['SIMPLE_DB_DOMAIN_NAME']

QUEUE_NAME = os.environ['QUEUE_NAME']
BUCKET_NAME = os.environ['BUCKET_NAME']

s3 = boto3.resource('s3', aws_access_key_id=AWS_ACCESS_KEY_ID,
                    aws_secret_access_key=AWS_SECRET_ACCESS_KEY)
sqs = boto3.resource('sqs', region_name='us-west-2',
                     aws_access_key_id=AWS_ACCESS_KEY_ID, aws_secret_access_key=AWS_SECRET_ACCESS_KEY)
queue = sqs.get_queue_by_name(QueueName=QUEUE_NAME)

sdb = boto3.client('sdb', region_name='us-west-2', aws_access_key_id=AWS_ACCESS_KEY_ID,
                   aws_secret_access_key=AWS_SECRET_ACCESS_KEY)

sdb.create_domain(DomainName=SIMPLE_DB_DOMAIN_NAME)
# sdb.delete_domain(DomainName=SIMPLE_DB_DOMAIN_NAME)
# log_image(sdb, SIMPLE_DB_DOMAIN_NAME, "test", 'False', time())


def get_images(client):
    response = client.select(
        SelectExpression='SELECT * FROM {}'.format(SIMPLE_DB_DOMAIN_NAME),
        ConsistentRead=True
    )
    print(response)


while True:
    messages = queue.receive_messages(MaxNumberOfMessages=os.cpu_count())
    for message in messages:
        filename = message.body.split('/')[-1]
        print(filename)
        s3.Bucket(BUCKET_NAME).download_file("uploads/" + filename, filename)

        img = skimage.io.imread(filename)
        new_img = invert(sobel(rgb2gray(img)))
        skimage.io.imsave(filename, new_img)

        s3.Bucket(BUCKET_NAME).upload_file(filename, 'edges/' + filename)
        log_image(sdb, SIMPLE_DB_DOMAIN_NAME, filename, 'True', time())
        message.delete()

        os.remove(filename)
