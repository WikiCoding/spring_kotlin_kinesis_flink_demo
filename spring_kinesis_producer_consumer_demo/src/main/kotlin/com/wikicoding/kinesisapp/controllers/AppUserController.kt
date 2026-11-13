package com.wikicoding.kinesisapp.controllers

import com.wikicoding.kinesisapp.client.KinesisProducer
import com.wikicoding.kinesisapp.models.AppUser
import com.wikicoding.kinesisapp.models.UserRole
import org.springframework.web.bind.annotation.GetMapping
import org.springframework.web.bind.annotation.PostMapping
import org.springframework.web.bind.annotation.RequestBody
import org.springframework.web.bind.annotation.RequestMapping
import org.springframework.web.bind.annotation.RestController
import tools.jackson.databind.ObjectMapper
import java.time.Instant

@RestController
@RequestMapping("/api")
class AppUserController(
    private val kinesisProducer: KinesisProducer
) {

    @PostMapping("/appuser")
    fun getAppUser(@RequestBody appUser: AppUser): AppUser {
        val json = ObjectMapper().writeValueAsString(appUser)
        kinesisProducer.putRecord("appuser-stream", json, Instant.now().toString())
        return appUser
    }

    @PostMapping("/userrole")
    fun getUserRole(@RequestBody userRole: UserRole): UserRole {
        val json = ObjectMapper().writeValueAsString(userRole)
        kinesisProducer.putRecord("userrole-stream", json, Instant.now().toString())
        return userRole
    }
}
