#!/bin/bash
#

set -e

#export PATH="$PATH:/usr/local/bin"

usage()
{
cat << EOF
usage: $0 options

This script dumps the current mongo database, then sends it to an Amazon S3 bucket.

OPTIONS:
   -h      Show this message
   -b      Amazon S3 bucket name
   -d      Mongo Database
   -c      Mongo Collection
   -w      Mongo Host
EOF
}

S3_BUCKET=
MONGO_DB=
MONGO_COLLECTION=
MONGO_HOST=
S3_DIR=

while getopts “ht:b:d:c:w:k:” OPTION
do
  case $OPTION in
    h)
      usage
      exit 1
      ;;
    b)
      S3_BUCKET=$OPTARG
      ;;
    d)
      MONGO_DB=$OPTARG
      ;;
    c)
      MONGO_COLLECTION=$OPTARG
      ;;
    w)
      MONGO_HOST=$OPTARG
      ;;
    k)
      S3_DIR=$OPTARG
      ;;
    ?)
      usage
      exit
    ;;
   esac
done

if [[ -z $S3_BUCKET ]] || [[ -z $MONGO_HOST ]] || [[ -z $MONGO_DB ]] || [[ -z $MONGO_COLLECTION ]]
then
  usage
  exit 1
fi

# Get the directory the script is being run from
DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
echo $DIR
# Store the current date in YYYY-mm-DD-HHMMSS
#DATE=$(date -u "+%F-%H%M%S")
DIR_NAME="$MONGO_COLLECTION-dump"
FILE_NAME="$MONGO_COLLECTION.bson"
#ARCHIVE_NAME="$FILE_NAME.tar.gz"

# Dump the database
mongodump -h "$MONGO_HOST" -d "$MONGO_DB" -c "$MONGO_COLLECTION"  --out $DIR/$DIR_NAME

# Removes previous data on S3
# aws s3 rm "s3://$S3_BUCKET/$S3_DIR$FILE_NAME"
# Uploads the database is dump to S3
aws s3 cp "$DIR/$DIR_NAME/backend/$FILE_NAME" "s3://$S3_BUCKET/$S3_DIR"

#Removes files locally:
rm -rf "$DIR/$DIR_NAME"

# Send the file to the backup drive or S3

#HEADER_DATE=$(date -u "+%a, %d %b %Y %T %z")
#CONTENT_MD5=$(openssl dgst -md5 -binary $DIR/$DIR_NAME/backend/$FILE_NAME | openssl enc -base64)
#CONTENT_TYPE="application/x-download"
#STRING_TO_SIGN="PUT\n$CONTENT_MD5\n$CONTENT_TYPE\n$HEADER_DATE\n/$S3_BUCKET/$DIR_NAME"
#SIGNATURE=$(echo -e -n $STRING_TO_SIGN | openssl dgst -sha1 -binary -hmac $AWS_SECRET_KEY | openssl enc -base64)

#curl -X PUT \
#--header "Host: $S3_BUCKET.s3-$S3_REGION.amazonaws.com" \
#--header "Date: $HEADER_DATE" \
#--header "content-type: $CONTENT_TYPE" \
#--header "Content-MD5: $CONTENT_MD5" \
#--header "Authorization: AWS $AWS_ACCESS_KEY:$SIGNATURE" \
#--upload-file $DIR/$DIR_NAME/backup/$FILE_NAME \
#https://$S3_BUCKET.s3-$S3_REGION.amazonaws.com/$FILE_NAME
       
