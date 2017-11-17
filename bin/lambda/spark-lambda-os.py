from __future__ import print_function
import boto3
import os
import sys
import uuid
import zipfile
import socket
import time
import logging
from subprocess import call

boto3.set_stream_logger(name='boto3', level=logging.ERROR)
boto3.set_stream_logger(name='botocore', level=logging.ERROR)
s3_client = boto3.client('s3')

def list_all_files():
    for f in os.listdir('/tmp'):
        print('/tmp/' + f)
    for f in os.listdir('/tmp/lambda'):
        print('/tmp/lambda/' + f)
    print("-----------------------")

def run_executor(spark_driver_hostname, spark_driver_port, spark_executor_cmdline, java_partial_cmdline, executor_partial_cmdline, java_extra_options):
    #cmdline = spark_executor_cmdline
    cmdline = java_partial_cmdline + java_extra_options + executor_partial_cmdline
    cmdline_arr = cmdline.split(' ')
    cmdline_arr = [x for x in cmdline_arr if x]
    print("START: Spark executor: " + cmdline)
    print(cmdline_arr)
    call(cmdline_arr)
    print("FINISH: Spark executor")

def handler(event, context):
    print('START: ')
    print(event)

    print(context.function_name)
    print(context.function_version)
    print(context.invoked_function_arn)
    print(context.memory_limit_in_mb)
    print(context.aws_request_id)
    print(context.log_group_name)
    print(context.log_stream_name)
    print(context.identity)

    spark_driver_hostname = event['sparkDriverHostname']
    spark_driver_port = event['sparkDriverPort']
    spark_executor_cmdline = event['sparkCommandLine']

    java_partial_cmdline = event['javaPartialCommandLine']
    executor_partial_cmdline = event['executorPartialCommandLine']
    java_extra_options = '-Dspark.lambda.awsRequestId=' + context.aws_request_id + ' ' + \
        '-Dspark.lambda.logGroupName=' + context.log_group_name + ' ' + \
        '-Dspark.lambda.logStreamName=' + context.log_stream_name + ' '

    if os.path.isfile("/tmp/lambda/spark-installed"):
        print("FAST PATH: Not downloading spark")
        print("Cleaning up old temporary data /tmp/spark-application*")
        call(['rm', '-rf', '/tmp/spark-application*'])
        print('START: executor')
        run_executor(spark_driver_hostname, spark_driver_port, spark_executor_cmdline,
            java_partial_cmdline, executor_partial_cmdline, java_extra_options)
        print('FINISH: executor')
        return {
            'output' : 'Fast path Handler done'
        }

    bucket = event['sparkS3Bucket']
    key = event['sparkS3Key']
    call(['rm', '-rf', '/tmp/*'])
    call(['mkdir', '-p', '/tmp/lambda'])
    download_path = '/tmp/lambda/spark-lambda.zip'
    print('START: Downloading spark tarball')

    print("Bucket - %s Key - %s" %(bucket, key))

    s3_client.download_file(bucket, key, download_path)
    list_all_files()
    print('Extracting spark tarball')
    zip_ref = zipfile.ZipFile(download_path, 'r')
    zip_ref.extractall('/tmp/lambda/')
    zip_ref.close()
    list_all_files()
    call(['df', '-h'])
    call(['rm', download_path])
    call(['rm', '-rf', '/tmp/lambda/spark/python/test_support/'])
    call(['rm', '-rf', '/tmp/lambda/spark/R/'])
    call(['rm', '-rf', '/tmp/lambda/spark/R/'])
    call(['df', '-h'])
    print('FINISH: Downloading spark tarball')

    print('START: executor')
    run_executor(spark_driver_hostname, spark_driver_port, spark_executor_cmdline,
        java_partial_cmdline, executor_partial_cmdline, java_extra_options)
    print('FINISH: executor')
    open('/tmp/lambda/spark-installed', 'a').close()
    print('FINISH')
    return {
        'output' : 'Handler done'
    }

