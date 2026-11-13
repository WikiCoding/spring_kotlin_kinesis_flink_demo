package com.wikicoding.kinesisapp.config

import org.springframework.beans.factory.annotation.Value
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider
import software.amazon.awssdk.regions.Region
import software.amazon.awssdk.services.kinesis.KinesisClient

@Configuration
class AwsKinesisConfig {

    @Value("\${aws.kinesis.endpoint:http://localhost:4566}")
    private lateinit var endpoint: String

    @Value("\${aws.region:us-east-1}")
    private val region: String = "us-east-1"

    @Value("\${aws.access-key:test}")
    private val accessKey: String = "test"

    @Value("\${aws.secret-key:test}")
    private val secretKey: String = "test"

    @Bean
    fun kinesisClient(): KinesisClient {
        return KinesisClient.builder()
            .endpointOverride(java.net.URI(endpoint))
            .region(Region.of(region))
            .credentialsProvider(
                StaticCredentialsProvider.create(
                    AwsBasicCredentials.create(accessKey, secretKey)
                )
            )
            .build()
    }
}
