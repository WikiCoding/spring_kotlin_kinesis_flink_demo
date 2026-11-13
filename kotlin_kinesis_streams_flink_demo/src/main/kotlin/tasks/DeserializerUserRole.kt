package org.example.tasks

import com.fasterxml.jackson.databind.ObjectMapper
import org.apache.flink.api.common.functions.RichMapFunction
import org.example.models.UserRole

class DeserializerUserRole(private val objectMapper: ObjectMapper) : RichMapFunction<String, UserRole>() {
    override fun map(value: String): UserRole =
        objectMapper.readValue(value, UserRole::class.java)
}
