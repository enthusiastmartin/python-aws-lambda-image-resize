from __future__ import print_function

import boto3

import os
import sys, traceback
import uuid

import logging
from PIL import Image
from resizeimage import resizeimage
from resizeimage.imageexceptions import ImageSizeError

DEBUG = False

TARGET_BuCKET_NAME = "target_bucket"
TARGET_BUCKET_LOCATION = "images/{filename}"

TARGET_RESIZE_SIZE = [400, 400]


class S3MockClient(object):
    def download_file(self, *args):
        logging.debug("Downloading file from s3 -  {bucket}/{key}".format(bucket=args[0], key=args[1]))
        import shutil
        shutil.copy2('test_data/test.jpg', args[2])

    def upload_file(self, *args):
        pass


class S3MockResrouce(object):

    class ObjectAclMock(object):
        def put(self, *args, **kwargs):
            pass

    def ObjectAcl(self, *args):
        return S3MockResrouce.ObjectAclMock()


def connect_s3():
    if DEBUG:
        return {'client': S3MockClient(), 'resource': S3MockResrouce()}
    return {'client': boto3.client('s3'), 'resource': boto3.resource('s3')}


def process_image(s3_conn, local_source_path, local_resize_path, desired_size, s3_path):

    with Image.open(local_source_path) as image:
        resized_image = resizeimage.resize_cover(image, desired_size)
        resized_image.save(local_resize_path, image.format)

    s3_conn['client'].upload_file(local_resize_path, '{bucket_name}'.format(bucket_name=TARGET_BuCKET_NAME), s3_path)
    object_acl = s3_conn['resource'].ObjectAcl('{bucket_name}'.format(bucket_name=TARGET_BuCKET_NAME), s3_path)
    response = object_acl.put(ACL='public-read')

    if DEBUG:
        logging.debug(response)
    else:
        # TODO: check response
        pass


def handler(event, context):
    s3_conn = connect_s3()

    for record in event['Records']:
        logging.debug('Processing {}'.format(str(record)))
        bucket = record['s3']['bucket']['name']

        key = record['s3']['object']['key']

        download_path = '/tmp/orig-{}_orig'.format(uuid.uuid4())
        local_resize_location = '/tmp/resized-{}'.format(uuid.uuid4())

        s3_conn['client'].download_file(bucket, key, download_path)

        s3_location = TARGET_BUCKET_LOCATION.format(filename=str(os.path.basename(key)))

        try:
            process_image(s3_conn, download_path, local_resize_location, TARGET_RESIZE_SIZE, s3_location)
        except FileNotFoundError:
            logging.error("Failed to resize {} - problem with storing file locally".format(key))
            # traceback.print_exc(file=sys.stdout)
        except ImageSizeError as e:
            logging.error(str(e))
        except Exception as e:
            logging.error(str(e))
            traceback.print_exc(file=sys.stderr)
        finally:
            logging.info("Cleaning up")

            try:
                os.remove(download_path)
            except OSError:
                # If it does not exist, pass
                pass

            try:
                os.remove(local_resize_location)
            except OSError:
                # If it does not exist, pass
                pass


if __name__ == '__main__':
    DEBUG = True
    logging.basicConfig(level=logging.DEBUG)

    record = {'s3': {'bucket': {'name': 'bucket_name'},
                     'object': {'key': 'object_key'}}}
    event = {'Records': [record]}

    handler(event, None)
