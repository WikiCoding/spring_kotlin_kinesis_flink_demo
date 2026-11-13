package com.wikicoding.kinesisapp.models

data class AppUser(
    val username: String,
    val email: String
)

data class UserRole(
    val username: String,
    val role: String
)
