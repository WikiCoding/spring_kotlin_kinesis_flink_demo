package com.wikicoding.kinesisapp

import org.springframework.boot.autoconfigure.SpringBootApplication
import org.springframework.boot.runApplication

@SpringBootApplication
class KinesisAppApplication

fun main(args: Array<String>) {
    runApplication<KinesisAppApplication>(*args)
}
