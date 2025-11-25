#!/bin/sh
set -e
awslocal s3 mb s3://rubin-summit
echo "S3 bucket rubin-summit created"
