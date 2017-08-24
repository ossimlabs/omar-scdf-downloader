#  omar-scdf-downloader
The Downloader is a Spring Cloud Data Flow (SCDF) Processor.
This means it:
1. Receives a message on a Spring Cloud input stream using Kafka.
2. Performs an operation on the data.
3. Sends the result on a Spring Cloud output stream using Kafka to a listening SCDF Processor or SCDF Sink.

## Purpose
The Downloader receives a JSON message from the Aggregator containing a list of files to download from an S3 bucket. The Downloader then downloads the files to a directory based on a year-month-day-hour timestamp and a UUID. An example of the directory structure the Downloader will create:
```
/data/2017/06/22/933657b1-6752-42dc-98d8-73ef95a5e780/
```
The Downloader then sends a message to the Extractor with the list of files successfully downloaded and their full filepaths.

## JSON Input Example (from the Aggregator)
```json
{
   "files":[
      {
         "bucket":"omar-dropbox",
         "filename":"12345/SCDFTestImages.zip"
      },
      {
         "bucket":"omar-dropbox",
         "filename":"12345/SCDFTestImages.zip_56734.email"
      }
   ]
}
```

## JSON Output Example (to the Extractor)
```json
{
   "files":[
      "/data/2017/06/22/09/933657b1-6752-42dc-98d8-73ef95a5e780/12345/SCDFTestImages.zip",
      "/data/2017/06/22/09/933657b1-6752-42dc-98d8-73ef95a5e780/12345/SCDFTestImages.zip_56734.email"
   ]
}
```
