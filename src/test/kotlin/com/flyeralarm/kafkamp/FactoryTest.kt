package com.flyeralarm.kafkamp

import com.flyeralarm.kafkamp.commands.Ask
import com.flyeralarm.kafkamp.commands.MergeAll
import com.flyeralarm.kafkamp.commands.Print
import com.flyeralarm.kafkamp.commands.PurgeAll
import io.confluent.kafka.serializers.KafkaAvroDeserializer
import io.mockk.every
import io.mockk.mockk
import io.mockk.mockkStatic
import io.mockk.unmockkStatic
import io.mockk.verify
import org.apache.kafka.clients.CommonClientConfigs
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.kafka.common.serialization.StringDeserializer
import org.slf4j.Logger
import picocli.CommandLine
import java.nio.file.Files
import java.nio.file.Paths
import java.util.Properties
import java.util.UUID
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertTrue

class FactoryTest {
    @Test
    fun `adds sub-commands to command line`() {
        val factory = Factory(CLI(), mockk(relaxed = true), mockk(relaxed = true))
        assertEquals(
            setOf("ask", "merge-all", "purge-all", "print"),
            factory.commandLine.subcommands.keys
        )
    }

    @Test
    fun `adds logging exception handler to command line`() {
        val exception = RuntimeException("test")
        val logger = mockk<Logger>(relaxed = true)

        val factory = Factory(CLI(), logger, mockk(relaxed = true))
        assertEquals(
            1,
            factory.commandLine.executionExceptionHandler.handleExecutionException(exception, mockk(), mockk())
        )

        verify(exactly = 1) {
            logger.error("Failed to execute command", exception)
        }
    }

    @Test
    fun `correctly builds shared properties from provided options without file`() {
        val options = CLI()
        options.additionalProperties = mapOf("additional.property" to "test")

        val factory = Factory(options, mockk(relaxed = true), mockk(relaxed = true))
        assertEquals(
            Properties().also {
                it["additional.property"] = "test"
            },
            factory.sharedProperties
        )
    }

    @Test
    fun `correctly builds shared properties from provided options with file`() {
        mockkStatic(Files::class)

        val options = CLI()
        options.propertiesFilePath = Paths.get("file.properties")
        options.additionalProperties = mapOf("additional.property" to "test")

        every { Files.newInputStream(any()) } returns "file.property=value".byteInputStream()

        val factory = Factory(options, mockk(relaxed = true), mockk(relaxed = true))
        assertEquals(
            Properties().also {
                it["file.property"] = "value"
                it["additional.property"] = "test"
            },
            factory.sharedProperties
        )

        unmockkStatic(Files::class)
    }

    @Test
    fun `correctly builds shared properties from provided options with file and additional properties override`() {
        mockkStatic(Files::class)

        val options = CLI()
        options.propertiesFilePath = Paths.get("file.properties")
        options.additionalProperties = mapOf("additional.property" to "test", "overridable.property" to "new")

        every { Files.newInputStream(any()) } returns "overridable.property=value".byteInputStream()

        val factory = Factory(options, mockk(relaxed = true), mockk(relaxed = true))
        assertEquals(
            Properties().also {
                it["overridable.property"] = "new"
                it["additional.property"] = "test"
            },
            factory.sharedProperties
        )

        unmockkStatic(Files::class)
    }

    @Test
    fun `correctly builds consumer properties from provided options and merges with shared`() {
        val options = CLI()
        options.consumerGroup = "consumer-group"
        options.additionalProperties = mapOf("additional.property" to "test", "overridable.property" to "pre")
        options.additionalConsumerProperties = mapOf(
            "consumer.property" to "testing",
            "overridable.property" to "after"
        )

        val factory = Factory(options, mockk(relaxed = true), mockk(relaxed = true))
        assertEquals(
            Properties().also {
                it["additional.property"] = "test"
                it["consumer.property"] = "testing"
                it["overridable.property"] = "after"

                it[ConsumerConfig.ISOLATION_LEVEL_CONFIG] = "read_committed"
                it[ConsumerConfig.AUTO_OFFSET_RESET_CONFIG] = "earliest"

                it[ConsumerConfig.GROUP_ID_CONFIG] = "consumer-group"
                it[ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG] = false

                it[ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG] = MixedValue.Deserializer::class.java
                it[ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG] = MixedValue.Deserializer::class.java

                it[MixedValue.Deserializer.KEY_DELEGATE_CONFIG] = StringDeserializer::class.java
                it[MixedValue.Deserializer.VALUE_DELEGATE_CONFIG] = StringDeserializer::class.java
            },
            factory.consumerProperties
        )
    }

    @Test
    fun `correctly builds consumer properties from provided options with file`() {
        mockkStatic(Files::class)

        val options = CLI()
        options.consumerGroup = "consumer-group"
        options.consumerPropertiesFilePath = Paths.get("consumer.properties")
        options.additionalConsumerProperties = mapOf("additional.property" to "test")

        every { Files.newInputStream(any()) } returns "file.property=value".byteInputStream()

        val factory = Factory(options, mockk(relaxed = true), mockk(relaxed = true))
        assertEquals(
            Properties().also {
                it["additional.property"] = "test"
                it["file.property"] = "value"

                it[ConsumerConfig.ISOLATION_LEVEL_CONFIG] = "read_committed"
                it[ConsumerConfig.AUTO_OFFSET_RESET_CONFIG] = "earliest"

                it[ConsumerConfig.GROUP_ID_CONFIG] = "consumer-group"
                it[ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG] = false

                it[ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG] = MixedValue.Deserializer::class.java
                it[ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG] = MixedValue.Deserializer::class.java

                it[MixedValue.Deserializer.KEY_DELEGATE_CONFIG] = StringDeserializer::class.java
                it[MixedValue.Deserializer.VALUE_DELEGATE_CONFIG] = StringDeserializer::class.java
            },
            factory.consumerProperties
        )

        unmockkStatic(Files::class)
    }

    @Test
    fun `additional consumer properties override those from file`() {
        mockkStatic(Files::class)

        val options = CLI()
        options.consumerGroup = "consumer-group"
        options.consumerPropertiesFilePath = Paths.get("consumer.properties")
        options.additionalConsumerProperties = mapOf("file.property" to "test")

        every { Files.newInputStream(any()) } returns "file.property=value".byteInputStream()

        val factory = Factory(options, mockk(relaxed = true), mockk(relaxed = true))
        assertEquals("test", factory.consumerProperties["file.property"])

        unmockkStatic(Files::class)
    }

    @Test
    fun `always uses mixed value deserializer in consumer properties`() {
        val options = CLI()
        options.consumerGroup = "consumer-group"
        options.additionalConsumerProperties = mapOf(
            ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG to "key_serializer",
            ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG to "value_serializer"
        )

        val factory = Factory(options, mockk(relaxed = true), mockk(relaxed = true))
        assertEquals(
            MixedValue.Deserializer::class.java,
            factory.consumerProperties[ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG]
        )
        assertEquals(
            MixedValue.Deserializer::class.java,
            factory.consumerProperties[ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG]
        )
    }

    @Test
    fun `always uses consumer group from option rather than properties`() {
        val options = CLI()
        options.consumerGroup = "consumer-group"
        options.additionalConsumerProperties = mapOf(
            ConsumerConfig.GROUP_ID_CONFIG to "try-to-override"
        )

        val factory = Factory(options, mockk(relaxed = true), mockk(relaxed = true))
        assertEquals("consumer-group", factory.consumerProperties[ConsumerConfig.GROUP_ID_CONFIG])
    }

    @Test
    fun `always sets auto-commit to false regardless of properties`() {
        val options = CLI()
        options.consumerGroup = "consumer-group"
        options.additionalConsumerProperties = mapOf(
            ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG to "true"
        )

        val factory = Factory(options, mockk(relaxed = true), mockk(relaxed = true))
        assertEquals(false, factory.consumerProperties[ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG])
    }

    @Test
    fun `overrides bootstrap servers in consumer properties if provided`() {
        val options = CLI()
        options.consumerGroup = "consumer-group"
        options.additionalConsumerProperties = mapOf(
            ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG to "testing"
        )
        options.bootstrapServers = "bootstrap"

        val factory = Factory(options, mockk(relaxed = true), mockk(relaxed = true))
        assertEquals("bootstrap", factory.consumerProperties[ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG])
    }

    @Test
    fun `overrides isolation level in consumer properties if provided`() {
        val options = CLI()
        options.consumerGroup = "consumer-group"
        options.additionalConsumerProperties = mapOf(
            ConsumerConfig.ISOLATION_LEVEL_CONFIG to "read_uncommitted"
        )

        val factory = Factory(options, mockk(relaxed = true), mockk(relaxed = true))
        assertEquals("read_uncommitted", factory.consumerProperties[ConsumerConfig.ISOLATION_LEVEL_CONFIG])
    }

    @Test
    fun `overrides auto offset reset in consumer properties if provided`() {
        val options = CLI()
        options.consumerGroup = "consumer-group"
        options.additionalConsumerProperties = mapOf(
            ConsumerConfig.AUTO_OFFSET_RESET_CONFIG to "none"
        )

        val factory = Factory(options, mockk(relaxed = true), mockk(relaxed = true))
        assertEquals("none", factory.consumerProperties[ConsumerConfig.AUTO_OFFSET_RESET_CONFIG])
    }

    @Test
    fun `moves provided key deserializer config to mixed value delegate option`() {
        val options = CLI()
        options.consumerGroup = "consumer-group"
        options.additionalConsumerProperties = mapOf(
            ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG to "key_deserializer"
        )

        val factory = Factory(options, mockk(relaxed = true), mockk(relaxed = true))
        assertEquals("key_deserializer", factory.consumerProperties[MixedValue.Deserializer.KEY_DELEGATE_CONFIG])
    }

    @Test
    fun `moves provided value deserializer config to mixed value delegate option`() {
        val options = CLI()
        options.consumerGroup = "consumer-group"
        options.additionalConsumerProperties = mapOf(
            ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG to "value_deserializer"
        )

        val factory = Factory(options, mockk(relaxed = true), mockk(relaxed = true))
        assertEquals(
            "value_deserializer",
            factory.consumerProperties[MixedValue.Deserializer.VALUE_DELEGATE_CONFIG]
        )
    }

    @Test
    fun `overrides specified key deserializer with Avro if requested`() {
        val options = CLI()
        options.consumerGroup = "consumer-group"
        options.additionalConsumerProperties = mapOf(
            ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG to "key_deserializer"
        )
        options.avroKeyDeserializer = true

        val factory = Factory(options, mockk(relaxed = true), mockk(relaxed = true))
        assertEquals(
            KafkaAvroDeserializer::class.java,
            factory.consumerProperties[MixedValue.Deserializer.KEY_DELEGATE_CONFIG]
        )
    }

    @Test
    fun `overrides specified value deserializer with Avro if requested`() {
        val options = CLI()
        options.consumerGroup = "consumer-group"
        options.additionalConsumerProperties = mapOf(
            ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG to "value_deserializer"
        )
        options.avroValueDeserializer = true

        val factory = Factory(options, mockk(relaxed = true), mockk(relaxed = true))
        assertEquals(
            KafkaAvroDeserializer::class.java,
            factory.consumerProperties[MixedValue.Deserializer.VALUE_DELEGATE_CONFIG]
        )
    }

    @Test
    fun `correctly builds producer properties from provided options and merges with shared`() {
        val options = CLI()
        options.additionalProperties = mapOf("additional.property" to "test", "overridable.property" to "pre")
        options.additionalProducerProperties = mapOf(
            "producer.property" to "testing",
            "overridable.property" to "after"
        )

        val factory = Factory(options, mockk(relaxed = true), mockk(relaxed = true))
        assertEquals(
            Properties().also {
                it["additional.property"] = "test"
                it["producer.property"] = "testing"
                it["overridable.property"] = "after"

                it[ProducerConfig.ACKS_CONFIG] = "all"
                it[ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG] = true

                it[ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG] = MixedValue.Serializer::class.java
                it[ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG] = MixedValue.Serializer::class.java
            },
            factory.producerProperties
        )
    }

    @Test
    fun `correctly builds producer properties from provided options with file`() {
        mockkStatic(Files::class)

        val options = CLI()
        options.producerPropertiesFilePath = Paths.get("producer.properties")
        options.additionalProducerProperties = mapOf("additional.property" to "test")

        every { Files.newInputStream(any()) } returns "file.property=value".byteInputStream()

        val factory = Factory(options, mockk(relaxed = true), mockk(relaxed = true))
        assertEquals(
            Properties().also {
                it["additional.property"] = "test"
                it["file.property"] = "value"

                it[ProducerConfig.ACKS_CONFIG] = "all"
                it[ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG] = true

                it[ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG] = MixedValue.Serializer::class.java
                it[ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG] = MixedValue.Serializer::class.java
            },
            factory.producerProperties
        )

        unmockkStatic(Files::class)
    }

    @Test
    fun `additional producer properties override those from file`() {
        mockkStatic(Files::class)

        val options = CLI()
        options.producerPropertiesFilePath = Paths.get("producer.properties")
        options.additionalProducerProperties = mapOf("file.property" to "test")

        every { Files.newInputStream(any()) } returns "file.property=value".byteInputStream()

        val factory = Factory(options, mockk(relaxed = true), mockk(relaxed = true))
        assertEquals("test", factory.producerProperties["file.property"])

        unmockkStatic(Files::class)
    }

    @Test
    fun `always uses mixed value serializer in producer properties`() {
        val options = CLI()
        options.additionalProducerProperties = mapOf(
            ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG to "key_serializer",
            ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG to "value_serializer"
        )

        val factory = Factory(options, mockk(relaxed = true), mockk(relaxed = true))
        assertEquals(
            MixedValue.Serializer::class.java,
            factory.producerProperties[ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG]
        )
        assertEquals(
            MixedValue.Serializer::class.java,
            factory.producerProperties[ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG]
        )
    }

    @Test
    fun `overrides bootstrap servers in producer properties if provided`() {
        val options = CLI()
        options.additionalProducerProperties = mapOf(
            CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG to "testing"
        )
        options.bootstrapServers = "bootstrap"

        val factory = Factory(options, mockk(relaxed = true), mockk(relaxed = true))
        assertEquals("bootstrap", factory.producerProperties[CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG])
    }

    @Test
    fun `overrides acks config in producer properties if provided`() {
        val options = CLI()
        options.additionalProducerProperties = mapOf(
            ProducerConfig.ACKS_CONFIG to "none"
        )

        val factory = Factory(options, mockk(relaxed = true), mockk(relaxed = true))
        assertEquals("none", factory.producerProperties[ProducerConfig.ACKS_CONFIG])
    }

    @Test
    fun `overrides idempotence config in producer properties if provided`() {
        val options = CLI()
        options.additionalProducerProperties = mapOf(
            ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG to "false"
        )

        val factory = Factory(options, mockk(relaxed = true), mockk(relaxed = true))
        assertEquals("false", factory.producerProperties[ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG])
    }

    @Test
    fun `generates random UUID if transactions are requested but no ID is set`() {
        val uuid = UUID.randomUUID()

        mockkStatic(UUID::class) {
            every { UUID.randomUUID() } returns uuid

            val options = CLI()
            options.transactionalId = ""

            val logger = mockk<Logger>(relaxed = true)

            val factory = Factory(options, logger, mockk(relaxed = true))
            assertEquals(uuid.toString(), factory.producerProperties[ProducerConfig.TRANSACTIONAL_ID_CONFIG])

            verify(exactly = 1) {
                logger.warn("Producer transactions were requested without transactional ID, using random UUID '$uuid'")
            }
        }
    }

    @Test
    fun `uses transaction ID from options if provided`() {
        val options = CLI()
        options.transactionalId = "transactional-id"

        val logger = mockk<Logger>(relaxed = true)

        val factory = Factory(options, logger, mockk(relaxed = true))
        assertEquals("transactional-id", factory.producerProperties[ProducerConfig.TRANSACTIONAL_ID_CONFIG])

        verify(exactly = 0) {
            logger.warn(any())
        }
    }

    @Test
    fun `uses transaction ID from properties if provided`() {
        val options = CLI()
        options.transactionalId = ""
        options.additionalProducerProperties = mapOf(ProducerConfig.TRANSACTIONAL_ID_CONFIG to "transaction")

        val logger = mockk<Logger>(relaxed = true)

        val factory = Factory(options, logger, mockk(relaxed = true))
        assertEquals("transaction", factory.producerProperties[ProducerConfig.TRANSACTIONAL_ID_CONFIG])

        verify(exactly = 0) {
            logger.warn(any())
        }
    }

    @Test
    fun `uses transaction ID from options rather than properties if both provided`() {
        val options = CLI()
        options.transactionalId = "cli"
        options.additionalProducerProperties = mapOf(ProducerConfig.TRANSACTIONAL_ID_CONFIG to "props")

        val logger = mockk<Logger>(relaxed = true)

        val factory = Factory(options, logger, mockk(relaxed = true))
        assertEquals("cli", factory.producerProperties[ProducerConfig.TRANSACTIONAL_ID_CONFIG])

        verify(exactly = 0) {
            logger.warn(any())
        }
    }

    @Test
    @Suppress("USELESS_IS_CHECK")
    fun `can create ask command without calling fallback factory`() {
        mockkStatic(CommandLine::class) {
            val fallbackFactory = mockk<CommandLine.IFactory>(relaxed = true)
            every { CommandLine.defaultFactory() } returns fallbackFactory

            val options = CLI()
            options.consumerGroup = "consumer-group"

            val factory = Factory(options, mockk(relaxed = true), mockk(relaxed = true), { mockk() }, { mockk() })
            assertTrue(factory.create(Ask::class.java) is Ask)

            verify(exactly = 0) {
                fallbackFactory.create(any())
            }
        }
    }

    @Test
    @Suppress("USELESS_IS_CHECK")
    fun `can create merge all command without calling fallback factory`() {
        mockkStatic(CommandLine::class) {
            val fallbackFactory = mockk<CommandLine.IFactory>(relaxed = true)
            every { CommandLine.defaultFactory() } returns fallbackFactory

            val options = CLI()
            options.consumerGroup = "consumer-group"

            val factory = Factory(options, mockk(relaxed = true), mockk(relaxed = true), { mockk() }, { mockk() })
            assertTrue(factory.create(MergeAll::class.java) is MergeAll)

            verify(exactly = 0) {
                fallbackFactory.create(any())
            }
        }
    }

    @Test
    @Suppress("USELESS_IS_CHECK")
    fun `can create purge all command without calling fallback factory`() {
        mockkStatic(CommandLine::class) {
            val fallbackFactory = mockk<CommandLine.IFactory>(relaxed = true)
            every { CommandLine.defaultFactory() } returns fallbackFactory

            val options = CLI()
            options.consumerGroup = "consumer-group"

            val factory = Factory(options, mockk(relaxed = true), mockk(relaxed = true), { mockk() }, { mockk() })
            assertTrue(factory.create(PurgeAll::class.java) is PurgeAll)

            verify(exactly = 0) {
                fallbackFactory.create(any())
            }
        }
    }

    @Test
    @Suppress("USELESS_IS_CHECK")
    fun `can create print command without calling fallback factory`() {
        mockkStatic(CommandLine::class) {
            val fallbackFactory = mockk<CommandLine.IFactory>(relaxed = true)
            every { CommandLine.defaultFactory() } returns fallbackFactory

            val options = CLI()
            options.consumerGroup = "consumer-group"

            val factory = Factory(options, mockk(relaxed = true), mockk(relaxed = true), { mockk() }, { mockk() })
            assertTrue(factory.create(Print::class.java) is Print)

            verify(exactly = 0) {
                fallbackFactory.create(any())
            }
        }
    }

    @Test
    fun `uses fallback factory for random non-subcommand class`() {
        mockkStatic(CommandLine::class) {
            val uuid = UUID.randomUUID()

            val fallbackFactory = mockk<CommandLine.IFactory>(relaxed = true)
            every { CommandLine.defaultFactory() } returns fallbackFactory
            every { fallbackFactory.create(UUID::class.java) } returns uuid

            val options = CLI()
            options.consumerGroup = "consumer-group"

            val factory = Factory(options, mockk(relaxed = true), mockk(relaxed = true), { mockk() }, { mockk() })
            assertEquals(uuid, factory.create(UUID::class.java))

            verify(exactly = 1) {
                fallbackFactory.create(UUID::class.java)
            }
        }
    }
}
