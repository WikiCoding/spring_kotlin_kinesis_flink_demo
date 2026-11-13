package org.example.models

import java.io.Serializable

data class AppUser(
    var username: String = "",
    var email: String = ""
) : Serializable
