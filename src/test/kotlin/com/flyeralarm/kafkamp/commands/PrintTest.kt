package com.flyeralarm.kafkamp.commands

import io.mockk.every
import io.mockk.just
import io.mockk.mockk
import io.mockk.runs
import io.mockk.verify
import io.mockk.verifyOrder
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.clients.consumer.ConsumerRecords
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.common.TopicPartition
import org.slf4j.Logger
import java.time.Duration
import kotlin.test.Test
import kotlin.test.assertEquals

class PrintTest {
    @Test
    fun `subscribes to consumer and polls until no more records are found`() {
        val consumer = mockk<KafkaConsumer<Any?, Any?>>()

        every { consumer.subscribe(any<Collection<String>>()) } just runs
        every { consumer.commitSync() } just runs
        every { consumer.close() } just runs

        every { consumer.poll(any<Duration>()) } returnsMany listOf(
            ConsumerRecords(mapOf(TopicPartition("test", 0) to listOf(ConsumerRecord("test", 0, 0, mockk(), mockk())))),
            ConsumerRecords(mapOf(TopicPartition("test", 0) to listOf(ConsumerRecord("test", 0, 0, mockk(), mockk())))),
            ConsumerRecords(emptyMap())
        )

        val command = Print(mockk(relaxed = true), consumer, false)
        command.topic = "test"

        assertEquals(0, command.call())

        verifyOrder {
            consumer.subscribe(listOf("test"))
            consumer.poll(any<Duration>())
            consumer.commitSync()
            consumer.poll(any<Duration>())
            consumer.commitSync()
            consumer.poll(any<Duration>())
            consumer.commitSync()
            consumer.close()
        }
    }

    @Test
    fun `does not commit offsets if requested`() {
        val consumer = mockk<KafkaConsumer<Any?, Any?>>()

        every { consumer.subscribe(any<Collection<String>>()) } just runs
        every { consumer.commitSync() } just runs
        every { consumer.close() } just runs

        every { consumer.poll(any<Duration>()) } returnsMany listOf(
            ConsumerRecords(mapOf(TopicPartition("test", 0) to listOf(ConsumerRecord("test", 0, 0, mockk(), mockk())))),
            ConsumerRecords(mapOf(TopicPartition("test", 0) to listOf(ConsumerRecord("test", 0, 0, mockk(), mockk())))),
            ConsumerRecords(emptyMap())
        )

        val command = Print(mockk(relaxed = true), consumer, true)
        command.topic = "test"

        assertEquals(0, command.call())

        verifyOrder {
            consumer.subscribe(listOf("test"))
            consumer.poll(any<Duration>())
            consumer.poll(any<Duration>())
            consumer.poll(any<Duration>())
            consumer.close()
        }
    }

    @Test
    fun `logs pretty-printed records`() {
        val consumer = mockk<KafkaConsumer<Any?, Any?>>(relaxed = true)

        every { consumer.poll(any<Duration>()) } returnsMany listOf(
            ConsumerRecords(mapOf(TopicPartition("test", 0) to listOf(ConsumerRecord("test", 0, 0, "KEY1", "VALUE1")))),
            ConsumerRecords(mapOf(TopicPartition("test", 1) to listOf(ConsumerRecord("test", 1, 1, "KEY2", "VALUE2")))),
            ConsumerRecords(emptyMap())
        )

        val logger = mockk<Logger>(relaxed = true)

        val command = Print(logger, consumer, false)
        command.topic = "test"

        assertEquals(0, command.call())

        verifyOrder {
            logger.info(
                """
                Record at offset #0 in topic 'test' (Partition #0):
                    Key:
                    KEY1
                    Value:
                    VALUE1
                """.trimIndent()
            )
            logger.info(
                """
                Record at offset #1 in topic 'test' (Partition #1):
                    Key:
                    KEY2
                    Value:
                    VALUE2
                """.trimIndent()
            )
        }
    }

    @Test
    fun `logs number of successfully printed records`() {
        val consumer = mockk<KafkaConsumer<Any?, Any?>>(relaxed = true)

        every { consumer.poll(any<Duration>()) } returnsMany listOf(
            ConsumerRecords(mapOf(TopicPartition("test", 0) to listOf(ConsumerRecord("test", 0, 0, "KEY1", "VALUE1")))),
            ConsumerRecords(mapOf(TopicPartition("test", 1) to listOf(ConsumerRecord("test", 1, 1, "KEY2", "VALUE2")))),
            ConsumerRecords(mapOf(TopicPartition("test", 1) to listOf(ConsumerRecord("test", 1, 1, "KEY2", "VALUE2")))),
            ConsumerRecords(emptyMap())
        )

        val logger = mockk<Logger>(relaxed = true)

        val command = Print(logger, consumer, false)
        command.topic = "test"

        assertEquals(0, command.call())

        verify(exactly = 1) {
            logger.info("Finished printing 3 record(s) from 'test'")
        }
    }

    @Test
    fun `exits with code 1 if consumers throws exception and logs total printed`() {
        val consumer = mockk<KafkaConsumer<Any?, Any?>>(relaxed = true)
        val exception = RuntimeException("Test")

        every { consumer.poll(any<Duration>()) } returns ConsumerRecords(
            mapOf(
                TopicPartition("test", 0) to listOf(
                    ConsumerRecord(
                        "test",
                        0,
                        0,
                        "KEY1",
                        "VALUE1"
                    )
                )
            )
        ) andThenThrows exception

        val logger = mockk<Logger>(relaxed = true)

        val command = Print(logger, consumer, false)
        command.topic = "test"

        assertEquals(1, command.call())

        verifyOrder {
            logger.error("Failed to print all records from topic 'test'", exception)
            logger.info("1 record(s) were printed")
        }
    }
}
