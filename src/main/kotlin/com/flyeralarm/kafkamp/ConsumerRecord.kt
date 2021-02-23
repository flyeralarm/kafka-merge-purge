package com.flyeralarm.kafkamp

import org.apache.kafka.clients.consumer.ConsumerRecord

fun <K : Any?, V : Any?> ConsumerRecord<K, V>.prettyPrint(indent: String) =
    (
        "Key:\n" +
            (key()?.toString() ?: "null") + "\n" +
            "Value:\n" +
            (value()?.toString() ?: "<tombstone>")
        ).prependIndent(indent)
