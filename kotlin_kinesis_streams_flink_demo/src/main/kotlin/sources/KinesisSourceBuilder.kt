package org.example.sources

import org.apache.flink.api.common.eventtime.WatermarkStrategy
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.configuration.Configuration
import org.apache.flink.connector.aws.config.AWSConfigOptions
import org.apache.flink.connector.kinesis.source.KinesisStreamsSource
import org.apache.flink.connector.kinesis.source.config.KinesisSourceConfigOptions.InitialPosition
import org.apache.flink.connector.kinesis.source.config.KinesisSourceConfigOptions.STREAM_INITIAL_POSITION
import org.apache.flink.connector.kinesis.source.enumerator.assigner.ShardAssignerFactory
import org.apache.flink.streaming.api.datastream.DataStream
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
import org.example.configs.Configs
import java.time.Duration

object KinesisSourceBuilder {
    fun buildKinesisSource(env: StreamExecutionEnvironment,
                           streamArn: String,
                           sourceName: String,
                           streamSourceUid: String): DataStream<String> {
        val sourcesConfig = Configuration()
        sourcesConfig.set(AWSConfigOptions.AWS_ENDPOINT_OPTION, Configs.AWS_ENDPOINT) // needed for localstack
        sourcesConfig.set(AWSConfigOptions.AWS_REGION_OPTION, Configs.AWS_REGION)
        sourcesConfig.set(STREAM_INITIAL_POSITION, InitialPosition.TRIM_HORIZON)

        val source = KinesisStreamsSource.builder<String>()
            .setStreamArn(streamArn)
            .setSourceConfig(sourcesConfig)
            .setDeserializationSchema(SimpleStringSchema())
            .setKinesisShardAssigner(ShardAssignerFactory.uniformShardAssigner())
            .build()

        return env.fromSource(
            source,
            WatermarkStrategy
                .forMonotonousTimestamps<String>()
                .withIdleness(Duration.ofMillis(Configs.WATERMARK_IDLE_TIMEOUT_MS)),
            sourceName
        )
            .returns(TypeInformation.of(String::class.java))
            .uid(streamSourceUid)
    }
}
