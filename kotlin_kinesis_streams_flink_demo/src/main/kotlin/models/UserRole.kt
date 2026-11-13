package org.example.models

import java.io.Serializable

data class UserRole(
    var username: String = "",
    var role: String = ""
) : Serializable
