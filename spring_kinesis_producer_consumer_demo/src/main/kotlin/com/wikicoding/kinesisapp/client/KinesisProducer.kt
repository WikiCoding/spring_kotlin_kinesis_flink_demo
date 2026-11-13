package com.wikicoding.kinesisapp.client

import org.springframework.beans.factory.annotation.Value
import org.springframework.stereotype.Component
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider
import software.amazon.awssdk.core.SdkBytes
import software.amazon.awssdk.regions.Region
import software.amazon.awssdk.services.kinesis.KinesisClient
import java.net.URI
import java.time.Instant

@Component
class KinesisProducer(
    @Value("\${aws.kinesis.endpoint:http://localhost:4566}")
    private val endpoint: String,

    @Value("\${aws.region:us-east-1}")
    private val region: String,

    @Value("\${aws.access-key:test}")
    private val accessKey: String,

    @Value("\${aws.secret-key:test}")
    private val secretKey: String
) {

    private val kinesisClient = KinesisClient.builder()
        .endpointOverride(URI(endpoint))
        .region(Region.of(region))
        .credentialsProvider(StaticCredentialsProvider.create(
            AwsBasicCredentials.create(accessKey, secretKey)
        ))
        .build()

    fun putRecord(streamName: String, data: ByteArray, partitionKey: String) {
        kinesisClient.putRecord(
            software.amazon.awssdk.services.kinesis.model.PutRecordRequest.builder()
                .streamName(streamName)
                .data(SdkBytes.fromByteArray(data))
                .partitionKey(partitionKey)
                .build()
        )
    }

    fun putRecord(streamName: String, data: String, partitionKey: String) {
        putRecord(streamName, data.toByteArray(), partitionKey)
    }

    fun putRecord(streamName: String, data: String) {
        putRecord(streamName, data, Instant.now().toString())
    }
}
