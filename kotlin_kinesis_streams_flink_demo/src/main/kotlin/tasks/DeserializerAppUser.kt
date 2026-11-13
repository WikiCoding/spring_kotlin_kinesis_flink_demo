package org.example.tasks

import com.fasterxml.jackson.databind.ObjectMapper
import org.apache.flink.api.common.functions.RichMapFunction
import org.example.models.AppUser

class DeserializerAppUser(private val objectMapper: ObjectMapper) : RichMapFunction<String, AppUser>() {
    override fun map(value: String): AppUser =
        objectMapper.readValue(value, AppUser::class.java)
}
