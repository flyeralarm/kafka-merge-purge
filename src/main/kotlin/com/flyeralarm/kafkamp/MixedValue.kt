package com.flyeralarm.kafkamp

import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.config.AbstractConfig
import org.apache.kafka.common.config.ConfigDef
import org.apache.kafka.common.config.ConfigDef.Importance
import org.apache.kafka.common.header.Headers
import org.apache.kafka.common.serialization.Deserializer as KafkaDeserializer
import org.apache.kafka.common.serialization.Serializer as KafkaSerializer

typealias Record = ConsumerRecord<MixedValue?, MixedValue?>

data class MixedValue(val raw: ByteArray, val value: Any) {
    override fun toString() = value.toString()

    override fun equals(other: Any?): Boolean {
        return when (other) {
            is MixedValue -> other.value == value
            else -> false
        }
    }

    override fun hashCode(): Int {
        return value.hashCode()
    }

    class Deserializer : KafkaDeserializer<MixedValue> {
        private lateinit var delegate: KafkaDeserializer<*>

        override fun configure(configs: MutableMap<String, *>, isKey: Boolean) {
            val configKey = if (isKey) KEY_DELEGATE_CONFIG else VALUE_DELEGATE_CONFIG

            delegate = Config(configs).getConfiguredInstance(configKey, KafkaDeserializer::class.java)
            delegate.configure(configs, isKey)
        }

        override fun deserialize(topic: String, data: ByteArray?): MixedValue? {
            return MixedValue(data ?: return null, delegate.deserialize(topic, data) ?: return null)
        }

        override fun deserialize(topic: String, headers: Headers, data: ByteArray?): MixedValue? {
            return MixedValue(data ?: return null, delegate.deserialize(topic, headers, data) ?: return null)
        }

        override fun close() {
            // May be called before configure is done if error occurs there
            if (this::delegate.isInitialized) {
                delegate.close()
            }
        }

        companion object {
            const val KEY_DELEGATE_CONFIG = "kafkamp.key.deserializer.delegate"
            const val VALUE_DELEGATE_CONFIG = "kafkamp.value.deserializer.delegate"

            private val CONFIG_DEF = ConfigDef()
                .defineInternal(
                    KEY_DELEGATE_CONFIG,
                    ConfigDef.Type.CLASS,
                    ConfigDef.NO_DEFAULT_VALUE,
                    Importance.HIGH
                )
                .defineInternal(
                    VALUE_DELEGATE_CONFIG,
                    ConfigDef.Type.CLASS,
                    ConfigDef.NO_DEFAULT_VALUE,
                    Importance.HIGH
                )
        }

        private class Config(originals: Map<String, *>) : AbstractConfig(
            CONFIG_DEF,
            originals
        )
    }

    class Serializer : KafkaSerializer<MixedValue?> {
        override fun serialize(topic: String, data: MixedValue?): ByteArray? {
            return data?.raw
        }
    }
}
