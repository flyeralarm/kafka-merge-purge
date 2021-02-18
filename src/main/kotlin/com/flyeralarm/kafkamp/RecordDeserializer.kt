package com.flyeralarm.kafkamp

import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.serialization.Deserializer

class RecordDeserializer(
    private val keyDeserializer: Deserializer<out Any?>,
    private val valueDeserializer: Deserializer<out Any?>
) {
    fun deserialize(record: ConsumerRecord<ByteArray?, ByteArray?>): Record {
        return Record(
            record,
            record.key()?.let { keyDeserializer.deserialize(record.topic(), record.headers(), it) },
            record.value()?.let { valueDeserializer.deserialize(record.topic(), record.headers(), it) }
        )
    }

    data class Record(val original: ConsumerRecord<ByteArray?, ByteArray?>, val key: Any?, val value: Any?) {
        fun prettyPrint(indent: String) = (
            "Key:\n" +
                (key?.toString() ?: "null") + "\n" +
                "Value:\n" +
                (value?.toString() ?: "<tombstone>")
            ).prependIndent(indent)
    }
}
