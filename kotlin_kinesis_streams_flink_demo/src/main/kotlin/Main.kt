package org.example

import com.fasterxml.jackson.databind.ObjectMapper
import org.apache.flink.configuration.CheckpointingOptions
import org.apache.flink.configuration.Configuration
import org.apache.flink.configuration.StateBackendOptions
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
import org.example.configs.Configs
import org.example.sinks.KinesisSinkBuilder
import org.example.sources.KinesisSourceBuilder
import org.example.tasks.DeserializerAppUser
import org.example.tasks.DeserializerUserRole
import org.example.tasks.EnrichmentProcessor

object Main {
    @JvmStatic
    fun main(args: Array<String>) {
        val objectMapper = ObjectMapper()
        val configuration = Configuration()
        configuration.set(StateBackendOptions.STATE_BACKEND, Configs.STATE_BACKEND_OPTION)
        configuration.set(CheckpointingOptions.CHECKPOINT_STORAGE, Configs.CHECKPOINT_STORAGE_VALUE)

        val env = StreamExecutionEnvironment.getExecutionEnvironment(configuration)
        env.configure(configuration)
        env.parallelism = Configs.JOB_PARALLELISM
        env.enableCheckpointing(Configs.CHECKPOINTING_INTERVAL_MS)

        val appUserKinesisStream = KinesisSourceBuilder.buildKinesisSource(
            env,
            Configs.APP_USER_STREAM_ARN,
            Configs.APP_USER_STREAM_SOURCE_NAME,
            Configs.APP_USER_STREAM_SOURCE_UID
        )

        val userRoleKinesisStream = KinesisSourceBuilder.buildKinesisSource(
            env,
            Configs.USER_ROLE_STREAM_ARN,
            Configs.USER_ROLE_STREAM_SOURCE_NAME,
            Configs.USER_ROLE_STREAM_SOURCE_UID
        )

        val enrichedDataStream = appUserKinesisStream
            .map(DeserializerAppUser(objectMapper))
            .keyBy { it.username }
            .connect(
                userRoleKinesisStream
                    .map(DeserializerUserRole(objectMapper))
                    .keyBy { it.username }
            )
            .process(EnrichmentProcessor())

        enrichedDataStream.print().name(Configs.ENRICHED_USER_DATA_STREAM_NAME)
            .uid(Configs.ENRICHED_USER_DATA_STREAM_UID)

        val sink = KinesisSinkBuilder.buildEnrichedSink()

        enrichedDataStream.map { objectMapper.writeValueAsString(it) }
            .sinkTo(sink)
            .name(Configs.USER_ENRICHED_SINK_NAME)
            .uid(Configs.USER_ENRICHED_SINK_UID)

        env.execute(Configs.USER_ENRICHMENT_JOB_NAME)
    }
}
