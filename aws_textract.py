import boto3
import time

## Textract APIs used - "start_document_text_detection"
def InvokeTextDetectJob(s3BucketName, objectName):
    client = boto3.client('textract')
    response = client.start_document_text_detection(
            DocumentLocation={
                      'S3Object': {
                                    'Bucket': s3BucketName,
                                    'Name': objectName
                                }
           },
    )
    return response["JobId"]

## Textract APIs used - "start_document_analysis"
def InvokeTextAnalyzeJob(s3BucketName, objectName):
    client = boto3.client('textract')
    response = client.start_document_analysis(
            DocumentLocation={
                      'S3Object': {
                                    'Bucket': s3BucketName,
                                    'Name': objectName
                                }
           },
           FeatureTypes=['QUERIES',],
           QueriesConfig={
                        'Queries': [
                            {'Text': 'name', 'Alias' : 'name',},
                            {'Text': 'email', 'Alias' : 'email',},
                            {'Text': 'phone number', 'Alias' : 'phone',},
            ]
          }
    )
    return response["JobId"]

def CheckDetectJobComplete(jobId):
    client = boto3.client('textract')
    response = client.get_document_text_detection(JobId=jobId)
    status = response["JobStatus"]
    print("Job status: {}".format(status))
    while(status == "IN_PROGRESS"):
        time.sleep(1)
        response = client.get_document_text_detection(JobId=jobId)
        status = response["JobStatus"]
        print("Job status: {}".format(status))
    return status

def CheckAnalyzeJobComplete(jobId):
    client = boto3.client('textract')
    response = client.get_document_analysis(JobId=jobId)
    status = response["JobStatus"]
    print("Job status: {}".format(status))
    while(status == "IN_PROGRESS"):
        time.sleep(1)
        response = client.get_document_analysis(JobId=jobId)
        status = response["JobStatus"]
        print("Job status: {}".format(status))
    return status

## Textract APIs used - "get_document_text_detection"
def GetDetectJobResults(jobId):
    pages = []
    client = boto3.client('textract')
    response = client.get_document_text_detection(JobId=jobId)
 
    pages.append(response)
    print("Resultset page recieved: {}".format(len(pages)))
    nextToken = None
    if('NextToken' in response):
        nextToken = response['NextToken']
        while(nextToken):
            response = client.get_document_text_detection(JobId=jobId, NextToken=nextToken)
            pages.append(response)
            print("Resultset page recieved: {}".format(len(pages)))
            nextToken = None
            if('NextToken' in response):
                nextToken = response['NextToken']
    return pages

## Textract APIs used - "get_document_analysis"
def GetAnalyzeJobResults(jobId):
    pages = []
    client = boto3.client('textract')
    response = client.get_document_analysis(JobId=jobId)
 
    pages.append(response)
    print("Resultset page recieved: {}".format(len(pages)))
    nextToken = None
    if('NextToken' in response):
        nextToken = response['NextToken']
        while(nextToken):
            response = client.get_document_analysis(JobId=jobId, NextToken=nextToken)
            pages.append(response)
            print("Resultset page recieved: {}".format(len(pages)))
            nextToken = None
            if('NextToken' in response):
                nextToken = response['NextToken']
    return pages

# S3 Document Data
s3BucketName = "textract-console-us-east-1-b3068279-c29c-4b50-98ad-f72fca692d25"
documentName = "d8fab482_339a_4b6d_828a_1529491daf8f_63e16c16eba87.pdf"

# Function invokes
jobId = InvokeTextAnalyzeJob(s3BucketName, documentName)
print("Started job with id: {}".format(jobId))
if(CheckAnalyzeJobComplete(jobId)):
    response = GetAnalyzeJobResults(jobId)
    # print(response)
    text = ""
    extractedData = []
    for resultPage in response:
        for item in resultPage["Blocks"]:
            if item["BlockType"] == "LINE":
                text += item["Text"] + " "
            if item["BlockType"] == "QUERY_RESULT":
                extractedData.append(item["Text"])
    
    print(text)
    print(extractedData)