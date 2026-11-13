package org.example.sinks

import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.connector.aws.config.AWSConfigConstants
import org.apache.flink.connector.kinesis.sink.KinesisStreamsSink
import org.example.configs.Configs
import java.util.Properties

object KinesisSinkBuilder {

    fun buildEnrichedSink(): KinesisStreamsSink<String> {
        val enrichedSinkProperties = Properties()
        enrichedSinkProperties[AWSConfigConstants.AWS_CREDENTIALS_PROVIDER] = Configs.AWS_CREDENTIALS_PROVIDER_TYPE // needed for localstack
        enrichedSinkProperties[AWSConfigConstants.AWS_ENDPOINT] = Configs.AWS_ENDPOINT // needed for localstack
        enrichedSinkProperties[AWSConfigConstants.AWS_REGION] = Configs.AWS_REGION
        enrichedSinkProperties[AWSConfigConstants.AWS_ACCESS_KEY_ID] = Configs.AWS_ACCESS_KEY_ID
        enrichedSinkProperties[AWSConfigConstants.AWS_SECRET_ACCESS_KEY] = Configs.AWS_SECRET_ACCESS_KEY

        return KinesisStreamsSink.builder<String>()
            .setKinesisClientProperties(enrichedSinkProperties)
            .setSerializationSchema(SimpleStringSchema())
            .setPartitionKeyGenerator { it.hashCode().toString() }
            .setStreamName(Configs.ENRICHED_STREAM_NAME)
            .build()
    }
}
