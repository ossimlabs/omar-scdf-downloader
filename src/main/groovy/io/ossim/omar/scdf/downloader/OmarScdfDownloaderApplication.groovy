package io.ossim.omar.scdf.downloader

import groovy.json.JsonOutput
import org.springframework.beans.factory.annotation.Value
import org.springframework.boot.SpringApplication
import org.springframework.boot.autoconfigure.SpringBootApplication
import org.springframework.cloud.stream.annotation.EnableBinding
import org.springframework.cloud.stream.annotation.StreamListener
import org.springframework.cloud.stream.messaging.Processor
import org.springframework.core.io.support.ResourcePatternResolver
import org.springframework.messaging.Message
import org.springframework.messaging.handler.annotation.SendTo
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.core.io.ResourceLoader
import org.springframework.core.io.Resource
import groovy.json.JsonSlurper
import groovy.json.JsonBuilder
import org.slf4j.Logger
import org.slf4j.LoggerFactory

/**
 * Created by adrake on 5/31/2017
 */

@SpringBootApplication
@EnableBinding(Processor.class)
class OmarScdfDownloaderApplication {

	/**
	 * The application logger
	 */
	private Logger logger = LoggerFactory.getLogger(this.getClass())

	/**
	 * Filepath passed in from application.properties
	 */
	@Value('${filepath}')
	String filepath

	/**
	 * ResouceLoader used to access the s3 bucket objects
	 */
	@Autowired
	private ResourceLoader resourceLoader

	/**
	 * Provides a URI for the s3
	 */
	@Autowired
	private ResourcePatternResolver resourcePatternResolver

	/**
	 * The main entry point of the SCDF Downloader application.
	 * @param args
	 */
	static void main(String[] args) {
		SpringApplication.run OmarScdfDownloaderApplication, args
	}

	/**
	 * Receives a message from the SCDF aggregator, downloads the files in the message
	 * and puts them in the filepath on the SCDF server
	 * @param message The message object from the SCDF aggregrator (in JSON)
	 * @return a JSON message of the files, and bucket that need to be downloaded
	 */
	@StreamListener(Processor.INPUT) @SendTo(Processor.OUTPUT)
	String transform(Message<?> message){

		if(logger.isDebugEnabled()){
			logger.debug("Message received: ${message}")
		}

		final def parsedJson = new JsonSlurper().parseText(message.payload)
		String s3Bucket
		String s3Filename
		// Loop through each received JSON file and download
		for(int i = 0; i < parsedJson.files; i++) {
			s3Bucket = parsedJson.files[i].bucket
			s3Filename = parsedJson.files[i].filename

				String keyName = "/"+"";

				AmazonS3 s3Client = new AmazonS3Client(new PropertiesCredentials(
						DownloadUploadedFile.class
								.getResourceAsStream("AwsCredentials.properties")));

				GetObjectRequest request = new GetObjectRequest(s3Bucket,
						keyName);
				S3Object object = s3Client.getObject(request);
				S3ObjectInputStream objectContent = object.getObjectContent();
				IOUtils.copy(objectContent, new FileOutputStream("D://upload//test.jpg"));
		}



        JsonBuilder filesToDownload

        def fileNameFromMessage = fileFromJson[0..fileFromJson.lastIndexOf('.') - 1]
		def fileExtensionFromMessage = fileFromJson[fileFromJson.lastIndexOf('.')..fileFromJson.length() - 1]

		if(logger.isDebugEnabled()){
			logger.debug("\n-- Parsed Message --\nfileName: ${fileNameFromMessage} \nfileExtension: ${fileExtensionFromMessage}\nbucketName: ${bucketName}\n")
		}

		// TODO:
		// This assumes we will always be looking for two files with the aggregator.  Should
		// we make it so that we can also look for one, or maybe three???
		if (fileExtension1 == fileExtensionFromMessage) {

			// Looks for the associated file.  Example: .txt
            def fileToLookFor = "${fileNameFromMessage}${fileExtension2}"

			def s3Uri = "s3://${bucketName}/${fileToLookFor}"

			Resource s3FileResource = this.resourcePatternResolver.getResource(s3Uri)

			if(s3FileResource.exists()){
                // The other file exists! Put both files in a JSON array to send to next processor

                filesToDownload = new JsonBuilder()

                // TODO: make this build an array of N files to download
                filesToDownload.files{
                    [{
                        bucket bucketName
                        filename "${fileNameFromMessage}${fileExtension1}"
                    },
                    {
                        bucket bucketName
                        filename "${fileNameFromMessage}${fileExtension2}"
                    }]
                }
			} else {
				logger.warn("""
					Received notification for file that does not exist:
					${s3FileResource.filename}
					""")
			}
		}

		if(logger.isDebugEnabled()){
			logger.debug("filesToDownload: ${filesToDownload}")
		}
		return filesToDownload.toString()
	}
}