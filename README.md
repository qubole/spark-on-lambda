# Spark on Lambda - README
----------------------------------------

AWS Lambda is a Function as a Service which is serverless, scales up quickly and bills usage at 100ms granularity. We thought it would be interesting to see if we can get Apache Spark run on Lambda. This is an interesting idea we had, in order to validate we just hacked it into a prototype to see if it works. We were able to make it work making some changes in Spark's scheduler and shuffle areas. Since AWS Lambda has a 5 minute max run time limit, we have to shuffle over an external storage. So we hacked the shuffle parts of Spark code to shuffle over an external storage like S3.

This is a prototype and its not battle tested possibly can have bugs. The changes are made against OS Apache Spark-2.1.0 version. We also have a fork of Spark-2.2.0 which has few bugs will be pushed here soon. We welcome contributions from developers.

### For users, who wants to try out:

Bring up an EC2 machine with AWS credentials to invoke lambda function (~/.aws/credentials) in a VPC. Right now we only support credentials file way of loading lambda credentials with AWSLambdaClient. The spark driver will run on this machine. Also configure a security group for this machine.

Spark on Lambda package for driver [s3://public-qubole/lambda/spark-2.1.0-bin-spark-lambda-2.1.0.tgz] - This can be downloaded to an ec2 instance where the driver can be launched as Driver is generally long running needs to run inside an EC2 instance

Create the Lambda function with name spark-lambda from AWS console using the  (https://github.com/qubole/spark-on-lambda/bin/lambda/spark-lambda-os.py) and configure lambda function’s VPC and subnet to be same as that of the EC2 machine. Right now we use private IPs to register with Spark driver but this can be fixed to use public IP there by the Spark driver even can run on Mac or PC or any VM. It would also be nice to have a Docker container having the package which works out of the box.  

Also configure the security group of the lambda function to be the same as that of the EC2 machine. Note: Lambda role should have access to [s3://public-qubole/]
Also if you want to copy the packages to your bucket, use 

```
aws s3 cp s3://s3://public-qubole/lambda/spark-lambda-149.zip s3://YOUR_BUCKET/
aws s3 cp s3://s3://public-qubole/lambda/spark-2.1.0-bin-spark-lambda-2.1.0.tgz s3://YOUR_BUCKET/
```

Spark on Lambda package for executor to be launched inside lambda [s3://public-qubole/lambda/spark-lambda-149.zip] - This will be used in the lambda (executor) side. In order to use this package on the lambda side, pass spark configs like below:
		
        1. spark.lambda.s3.bucket s3://public-qubole/
        2. spark.lambda.function.name spark-lambda
        3. spark.lambda.spark.software.version 149

## Launch spark-shell

```
/usr/lib/spark/bin/spark-shell --conf spark.hadoop.fs.s3n.awsAccessKeyId= --conf spark.hadoop.fs.s3n.awsSecretAccessKey=
```

## Spark on Lambda configs (spark-defaults.conf)

```
spark.shuffle.s3.enabled true
spark.shuffle.s3.bucket s3://  -- Bucket to write shuffle (intermediate) data
spark.lambda.s3.bucket s3://public-qubole/  
spark.lambda.concurrent.requests.max 50
spark.lambda.function.name spark-lambda
spark.lambda.spark.software.version 149
spark.hadoop.fs.s3n.impl org.apache.hadoop.fs.s3a.S3AFileSystem
spark.hadoop.fs.s3.impl org.apache.hadoop.fs.s3a.S3AFileSystem
spark.hadoop.fs.AbstractFileSystem.s3.impl org.apache.hadoop.fs.s3a.S3A
spark.hadoop.fs.AbstractFileSystem.s3n.impl org.apache.hadoop.fs.s3a.S3A
spark.hadoop.fs.AbstractFileSystem.s3a.impl org.apache.hadoop.fs.s3a.S3A
spark.dynamicAllocation.enabled true
spark.dynamicAllocation.minExecutors 2
```

For developers, who wants to make changes:

## To compile

```
./dev/make-distribution.sh --name spark-lambda-2.1.0 --tgz -Phive -Phadoop-2.7 -Dhadoop.version=2.6.0-qds-0.4.13 -DskipTests 
```

Due to aws-java-sdk-1.7.4.jar which is used by hadoop-aws.jar and aws-java-sdk-core-1.1.0.jar has compatibility issues, so as of now we have to compile it using Qubole shaded hadoop-aws-2.6.0-qds-0.4.13.jar.

## To create lambda package for executors

```
bash -x bin/lambda/spark-lambda 149 (spark.lambda.spark.software.version) spark-2.1.0-bin-spark-lambda-2.1.0.tgz [s3://public-qubole/] (this maps to the config value of spark.lambda.s3.bucket)
```

(spark/bin/lambda/spark-lambda-os.py) is the helper lambda function used to bootstrap lambda environment with necessary Spark packages to run executors. 

Above Lambda function has to be created inside VPC which is same as the EC2 instance where driver is brought up for having communication between Driver and Executors (lambda function)


## References

1. http://deploymentzone.com/2015/12/20/s3a-on-spark-on-aws-ec2/

