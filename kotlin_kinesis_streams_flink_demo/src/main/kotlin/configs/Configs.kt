package org.example.configs

object Configs {
    const val AWS_ENDPOINT = "http://host.docker.internal:4566"
    const val AWS_CREDENTIALS_PROVIDER_TYPE = "BASIC"
    const val AWS_SECRET_ACCESS_KEY = "test"
    const val AWS_ACCESS_KEY_ID = "test"
    const val AWS_REGION = "us-east-1"
    const val CHECKPOINTING_INTERVAL_MS = 3600_000L // 1 hour
    const val APP_USER_STREAM_ARN = "arn:aws:kinesis:us-east-1:000000000000:stream/appuser-stream"
    const val APP_USER_STREAM_SOURCE_NAME = "AppUserKinesisSource"
    const val APP_USER_STREAM_SOURCE_UID = "AppUserKinesisSource-uid"
    const val USER_ROLE_STREAM_SOURCE_NAME = "UserRoleKinesisSource"
    const val USER_ROLE_STREAM_SOURCE_UID = "UserRoleKinesisSource-uid"
    const val USER_ROLE_STREAM_ARN = "arn:aws:kinesis:us-east-1:000000000000:stream/userrole-stream"
    const val STATE_TTL_DURATION_HOURS = 24L
    const val ENRICHED_STREAM_NAME = "user-enriched-stream"
    const val JOB_PARALLELISM = 1
    const val STATE_BACKEND_OPTION = "hashmap"
    const val CHECKPOINT_STORAGE_VALUE = "jobmanager"
    const val ENRICHED_USER_DATA_STREAM_NAME = "EnrichedUserStream"
    const val ENRICHED_USER_DATA_STREAM_UID = "EnrichedUserStream-uid"
    const val USER_ENRICHED_SINK_NAME = "UserEnrichedKinesisSink"
    const val USER_ENRICHED_SINK_UID = "UserEnrichedKinesisSink-uid"
    const val USER_ENRICHMENT_JOB_NAME = "UserEnrichmentJob"
    const val WATERMARK_IDLE_TIMEOUT_MS = 1000L
    const val MAX_TIME_IN_BUFFER_MS = 50L
}
