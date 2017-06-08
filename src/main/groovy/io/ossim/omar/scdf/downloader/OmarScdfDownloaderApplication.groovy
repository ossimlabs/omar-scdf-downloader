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
import com.amazonaws.AmazonClientException;
import com.amazonaws.AmazonServiceException;
import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.PutObjectRequest;
import com.amazonaws.services.s3.model.ObjectMetadata
import com.amazonaws.services.s3.model.GetObjectRequest

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

	@Value('${cloud.aws.credentials.accessKey')
	String accessKey

	@Value('${cloud.aws.credentials.secretKey')
	String secretKey

	AmazonS3Client s3Client = new AmazonS3Client(accessKey, secretKey)


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
	String download(Message<?> message){

		if(logger.isDebugEnabled()){
			logger.debug("Message received: ${message}")
		}

		final def parsedJson = new JsonSlurper().parseText(message.payload)
		String s3Bucket
		String s3Filename
		String files = new String [parsedJson.files];

		// Loop through each received JSON file and download
		for(int i = 0; i < parsedJson.files; i++) {
			s3Bucket = parsedJson.files[i].bucket
			s3Filename = parsedJson.files[i].filename
			files[i] = s3Filename
			File localFile = new File(filepath+s3Filename);
			ObjectMetadata object = s3Client.getObject(new GetObjectRequest(s3Bucket, s3Filename), localFile);

		}

    //    JsonBuilder filesDownloaded

		// Create a new instance of Gson
//		Gson filesDownloaded = new Gson();

		if(logger.isDebugEnabled()){
			logger.debug("\n-- files: ${files} \n")
		}

//		String filesDownloadedString = filesDownloaded.toJson(days);



		if(logger.isDebugEnabled()){
			logger.debug("filesDownloaded: ${filesDownloaded}")
		}
		return filesDownloadedString()
	}
}