package com.wikicoding.kinesisapp.consumer

import com.wikicoding.kinesisapp.service.KinesisStreamManager
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.Job
import kotlinx.coroutines.delay
import kotlinx.coroutines.launch
import org.slf4j.LoggerFactory
import org.springframework.beans.factory.annotation.Value
import org.springframework.stereotype.Component
import software.amazon.awssdk.services.kinesis.KinesisClient
import software.amazon.awssdk.services.kinesis.model.GetRecordsRequest
import software.amazon.awssdk.services.kinesis.model.GetShardIteratorRequest
import software.amazon.awssdk.services.kinesis.model.ShardIteratorType
import jakarta.annotation.PostConstruct
import jakarta.annotation.PreDestroy
import kotlinx.coroutines.cancel

private val log = LoggerFactory.getLogger(KinesisConsumer::class.java)

@Component
class KinesisConsumer(
    private val kinesisClient: KinesisClient,
    private val kinesisStreamManager: KinesisStreamManager,

    @Value("\${kinesis.streams:appuser-stream,userrole-stream}")
    private val streamNames: String,

    @Value("\${kinesis.consumer.iterator-type:TRIM_HORIZON}")
    private val iteratorType: String,

    @Value("\${kinesis.consumer.poll-limit:100}")
    private val pollLimit: Int,

    @Value("\${kinesis.consumer.poll-delay-ms:1000}")
    private val pollDelayMs: Long,

    @Value("\${kinesis.consumer.error-retry-delay-ms:5000}")
    private val errorRetryDelayMs: Long
) {

    private val scope = CoroutineScope(Dispatchers.IO + Job())

    @PostConstruct
    fun startConsuming() {
        kinesisStreamManager.ensureStreamsExist()
        streamNames.split(",").forEach { streamName ->
            scope.launch { consumeStream(streamName.trim()) }
        }
    }

    @PreDestroy
    fun shutdown() {
        log.info("Shutting down Kinesis consumer")
        scope.cancel()
    }

    private suspend fun consumeStream(streamName: String) {
        log.info("Started consuming stream: {}", streamName)
        var shardIterator: String? = getInitialShardIterator(streamName)

        while (shardIterator != null) {
            try {
                val response = kinesisClient.getRecords(
                    GetRecordsRequest.builder()
                        .shardIterator(shardIterator)
                        .limit(pollLimit)
                        .build()
                )

                response.records().forEach { record ->
                    log.info(
                        "[{}] streamName={}, partitionKey={}, message={}",
                        record.sequenceNumber(),
                        streamName,
                        record.partitionKey(),
                        record.data().asString(Charsets.UTF_8)
                    )
                }

                // Use the nextShardIterator from the response — this advances through the stream
                // instead of re-reading from the beginning every time
                shardIterator = response.nextShardIterator()

                // If no records were returned, wait before polling again
                if (response.records().isEmpty()) {
                    delay(pollDelayMs)
                }
            } catch (e: Exception) {
                log.error("Error consuming stream {}: {}", streamName, e.message, e)
                delay(errorRetryDelayMs)

                // On error, re-fetch the initial iterator to recover
                shardIterator = getInitialShardIterator(streamName)
            }
        }
    }

    private fun getInitialShardIterator(streamName: String): String? {
        val shards = kinesisClient.listShards { it.streamName(streamName) }
        val shardId = shards.shards().firstOrNull()?.shardId()
            ?: run { log.warn("No shards found for stream: {}", streamName); return null }

        return kinesisClient.getShardIterator(
            GetShardIteratorRequest.builder()
                .streamName(streamName)
                .shardId(shardId)
                .shardIteratorType(ShardIteratorType.fromValue(iteratorType))
                .build()
        ).shardIterator()
    }
}
