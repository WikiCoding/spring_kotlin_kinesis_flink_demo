package com.wikicoding.kinesisapp.service

import org.slf4j.LoggerFactory
import org.springframework.beans.factory.annotation.Value
import org.springframework.stereotype.Service
import software.amazon.awssdk.services.kinesis.KinesisClient
import software.amazon.awssdk.services.kinesis.model.CreateStreamRequest
import software.amazon.awssdk.services.kinesis.model.ResourceInUseException

private val log = LoggerFactory.getLogger(KinesisStreamManager::class.java)

@Service
class KinesisStreamManager(
    private val kinesisClient: KinesisClient,

    @Value("\${kinesis.streams:appuser-stream,userrole-stream}")
    private val streamNames: String,

    @Value("\${kinesis.stream-shard-count:1}")
    private val shardCount: Int
) {

    fun ensureStreamsExist() {
        streamNames.split(",").forEach { streamName ->
            ensureStreamExists(streamName.trim())
        }
    }

    private fun ensureStreamExists(streamName: String) {
        try {
            kinesisClient.createStream(
                CreateStreamRequest.builder()
                    .streamName(streamName)
                    .shardCount(shardCount)
                    .build()
            )
            log.info("Created stream: {}", streamName)
        } catch (e: ResourceInUseException) {
            log.info("Stream already exists: {}", streamName)
        }
    }
}
