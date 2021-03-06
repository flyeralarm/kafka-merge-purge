package com.flyeralarm.kafkamp

import com.flyeralarm.kafkamp.commands.Ask
import com.flyeralarm.kafkamp.commands.MergeAll
import com.flyeralarm.kafkamp.commands.Print
import com.flyeralarm.kafkamp.commands.PurgeAll
import com.flyeralarm.kafkamp.commands.streamActionSource
import io.confluent.kafka.serializers.KafkaAvroDeserializer
import org.apache.kafka.clients.CommonClientConfigs
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.kafka.common.serialization.StringDeserializer
import org.slf4j.Logger
import picocli.CommandLine
import java.nio.file.Files
import java.nio.file.Path
import java.util.Properties
import java.util.UUID

class Factory(
    private val options: CLI,
    private val logger: Logger,
    private val promptLogger: Logger,
    private val consumerProvider: (Properties) -> KafkaConsumer<MixedValue?, MixedValue?> = ::KafkaConsumer,
    private val producerProvider: (Properties) -> KafkaProducer<MixedValue?, MixedValue?> = ::KafkaProducer
) : CommandLine.IFactory {
    private val fallbackFactory = CommandLine.defaultFactory()

    val commandLine by lazy {
        CommandLine(options, this)
            .setExecutionExceptionHandler { ex, _, _ ->
                logger.error("Failed to execute command", ex)

                return@setExecutionExceptionHandler 1
            }
            .addSubcommand(Ask::class.java)
            .addSubcommand(MergeAll::class.java)
            .addSubcommand(PurgeAll::class.java)
            .addSubcommand(Print::class.java)
    }

    val sharedProperties = buildProperties(options.propertiesFilePath, options.additionalProperties).toMap()
    val consumerProperties by lazy {
        Properties().also {
            it[ConsumerConfig.ISOLATION_LEVEL_CONFIG] = "read_committed"
            it[ConsumerConfig.AUTO_OFFSET_RESET_CONFIG] = "earliest"
            it[ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG] = StringDeserializer::class.java
            it[ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG] = StringDeserializer::class.java

            it.putAll(sharedProperties)
            it.putAll(buildProperties(options.consumerPropertiesFilePath, options.additionalConsumerProperties))

            if (options.bootstrapServers != null) {
                it[CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG] = options.bootstrapServers
            }

            if (options.avroKeyDeserializer) {
                it[ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG] = KafkaAvroDeserializer::class.java
            }

            if (options.avroValueDeserializer) {
                it[ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG] = KafkaAvroDeserializer::class.java
            }

            if (it.containsKey(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG)) {
                it[MixedValue.Deserializer.KEY_DELEGATE_CONFIG] = it[ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG]
            }

            if (it.containsKey(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG)) {
                it[MixedValue.Deserializer.VALUE_DELEGATE_CONFIG] = it[ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG]
            }

            it[ConsumerConfig.GROUP_ID_CONFIG] = options.consumerGroup
            it[ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG] = false

            it[ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG] = MixedValue.Deserializer::class.java
            it[ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG] = MixedValue.Deserializer::class.java
        }
    }

    val producerProperties by lazy {
        Properties().also {
            it[ProducerConfig.ACKS_CONFIG] = "all"
            it[ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG] = true

            it.putAll(sharedProperties)
            it.putAll(buildProperties(options.producerPropertiesFilePath, options.additionalProducerProperties))

            if (options.bootstrapServers != null) {
                it[CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG] = options.bootstrapServers
            }

            it[ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG] = MixedValue.Serializer::class.java
            it[ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG] = MixedValue.Serializer::class.java

            if (options.transactionalId != null) {
                val transactionalId = options.transactionalId?.takeIf { id -> id.isNotEmpty() }
                    ?: it[ProducerConfig.TRANSACTIONAL_ID_CONFIG]
                    ?: UUID.randomUUID().toString().also { id ->
                        logger.warn("Producer transactions were requested without transactional ID, using random UUID '$id'")
                    }
                it[ProducerConfig.TRANSACTIONAL_ID_CONFIG] = transactionalId
            }
        }
    }

    private val consumer by lazy { consumerProvider(consumerProperties) }
    private val producer by lazy { producerProvider(producerProperties) }

    private val pipeline by lazy {
        Pipeline(
            consumer,
            producer,
            producerProperties.containsKey(ProducerConfig.TRANSACTIONAL_ID_CONFIG),
            options.noCommit ?: false
        )
    }

    @Suppress("UNCHECKED_CAST")
    override fun <K : Any?> create(cls: Class<K>): K =
        when (cls) {
            Ask::class.java -> Ask(logger, pipeline, streamActionSource(System.`in`, logger, promptLogger)) as K
            MergeAll::class.java -> MergeAll(logger, pipeline) as K
            PurgeAll::class.java -> PurgeAll(logger, pipeline) as K
            Print::class.java -> Print(logger, consumer, options.noCommit ?: true) as K
            else -> fallbackFactory.create(cls)
        }

    private fun buildProperties(filePath: Path?, additionalProperties: Map<String, String>): Properties {
        val result = Properties()

        if (filePath !== null) {
            Files.newInputStream(filePath).use {
                result.load(it)
            }
        }

        result.putAll(additionalProperties)

        return result
    }
}
