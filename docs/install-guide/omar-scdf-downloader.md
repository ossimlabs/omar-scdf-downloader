#  omar-scdf-downloader

## Pre-Req to running

1. Have a SCDF server set up 
2. Have a server with the following 3 microservices running: omar-scdf-server, omar-scdf-zookeeper, and omar-scdf-kafka

## Sample application.properties file
```
spring.cloud.stream.bindings.input.destination=sqs-aggregated
spring.cloud.stream.bindings.output.destination=files-downloaded
spring.cloud.stream.bindings.output.content.type=application/json
server.port=0

logging.level.org.springframework.web=ERROR
logging.level.io.ossim.omar.scdf.downloader=DEBUG

filepath=/data

# Don't use Cloud Formation
cloud.aws.stack.auto=false
cloud.aws.credentials.instanceProfile=true
cloud.aws.region.auto=true

#cloud.aws.credentials.accessKey=a
#cloud.aws.credentials.secretKey=b
```