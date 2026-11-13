package org.example.models

import java.io.Serializable

data class AppUserWithRole(
    var username: String = "",
    var email: String? = null,
    var role: String? = null
) : Serializable
