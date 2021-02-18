package com.flyeralarm.kafkamp

import io.mockk.every
import io.mockk.just
import io.mockk.mockk
import io.mockk.runs
import io.mockk.slot
import io.mockk.verify
import io.mockk.verifyOrder
import org.apache.kafka.clients.consumer.ConsumerGroupMetadata
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.clients.consumer.ConsumerRecords
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.clients.consumer.OffsetAndMetadata
import org.apache.kafka.clients.producer.Callback
import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.TopicPartition
import java.time.Duration
import kotlin.test.Test
import kotlin.test.assertEquals

class PipelineTest {
    @Test
    fun `subscribes to specified topic and polls from consumer until no more records are returned, commits after every iteration`() {
        val consumer = mockk<KafkaConsumer<Any?, Any?>>()

        every { consumer.subscribe(any<Collection<String>>()) } just runs
        every { consumer.commitSync() } just runs
        every { consumer.close() } just runs

        every { consumer.poll(any<Duration>()) } returnsMany listOf(
            ConsumerRecords(mapOf(TopicPartition("test", 0) to listOf(ConsumerRecord("test", 0, 0, mockk(), mockk())))),
            ConsumerRecords(mapOf(TopicPartition("test", 0) to listOf(ConsumerRecord("test", 0, 0, mockk(), mockk())))),
            ConsumerRecords(emptyMap())
        )

        val pipeline = Pipeline(consumer, mockk(relaxed = true), false, false)

        pipeline.processTopic("test") {}

        verifyOrder {
            consumer.subscribe(listOf("test"))
            consumer.poll(any<Duration>())
            consumer.commitSync()
            consumer.poll(any<Duration>())
            consumer.commitSync()
            consumer.poll(any<Duration>())
            consumer.close()
        }
    }

    @Test
    fun `calls callback for every record sequentially`() {
        val consumer = mockk<KafkaConsumer<Any?, Any?>>(relaxed = true)
        val producer = mockk<KafkaProducer<Any?, Any?>>(relaxed = true)

        val record1 = ConsumerRecord<Any?, Any?>("test", 0, 0, mockk(), mockk())
        val record2 = ConsumerRecord<Any?, Any?>("test", 0, 0, mockk(), mockk())
        val record3 = ConsumerRecord<Any?, Any?>("test", 0, 0, mockk(), mockk())

        every { consumer.poll(any<Duration>()) } returnsMany listOf(
            ConsumerRecords(mapOf(TopicPartition("test", 0) to listOf(record1, record2))),
            ConsumerRecords(mapOf(TopicPartition("test", 0) to listOf(record3))),
            ConsumerRecords(emptyMap())
        )

        val pipeline = Pipeline(consumer, producer, false, false)

        val usedRecords = mutableListOf<ConsumerRecord<Any?, Any?>>()
        pipeline.processTopic("test") {
            usedRecords += it
        }

        assertEquals(listOf(record1, record2, record3), usedRecords)
    }

    @Test
    fun `flushes producer after every iteration if not transactional`() {
        val consumer = mockk<KafkaConsumer<Any?, Any?>>(relaxed = true)
        val producer = mockk<KafkaProducer<Any?, Any?>>(relaxed = true)

        every { consumer.poll(any<Duration>()) } returnsMany listOf(
            ConsumerRecords(mapOf(TopicPartition("test", 0) to listOf(ConsumerRecord("test", 0, 0, mockk(), mockk())))),
            ConsumerRecords(emptyMap())
        )

        val pipeline = Pipeline(consumer, producer, false, false)

        pipeline.processTopic("test") {}

        verifyOrder {
            producer.flush()
            consumer.commitSync()
        }
    }

    @Test
    fun `uses producer transactions when specified`() {
        val consumer = mockk<KafkaConsumer<Any?, Any?>>(relaxed = true)
        val producer = mockk<KafkaProducer<Any?, Any?>>(relaxed = true)

        every { consumer.poll(any<Duration>()) } returnsMany listOf(
            ConsumerRecords(mapOf(TopicPartition("test", 0) to listOf(ConsumerRecord("test", 0, 0, mockk(), mockk())))),
            ConsumerRecords(emptyMap())
        )

        val pipeline = Pipeline(consumer, producer, true, false)

        pipeline.processTopic("test") {}

        verifyOrder {
            producer.initTransactions()
            producer.beginTransaction()
            producer.commitTransaction()
        }
    }

    @Test
    fun `commits offsets with transaction if using transaction`() {
        val consumer = mockk<KafkaConsumer<Any?, Any?>>(relaxed = true)
        val producer = mockk<KafkaProducer<Any?, Any?>>(relaxed = true)
        val consumerMetadata = mockk<ConsumerGroupMetadata>()

        every { consumer.groupMetadata() } returns consumerMetadata

        every { consumer.poll(any<Duration>()) } returnsMany listOf(
            ConsumerRecords(mapOf(TopicPartition("test", 0) to listOf(ConsumerRecord("test", 0, 0, mockk(), mockk())))),
            ConsumerRecords(emptyMap())
        )

        val pipeline = Pipeline(consumer, producer, true, false)

        pipeline.processTopic("test") {}

        verify(exactly = 1) {
            producer.sendOffsetsToTransaction(
                mapOf(TopicPartition("test", 0) to OffsetAndMetadata(0)),
                consumerMetadata
            )
        }
    }

    @Test
    fun `skips commit if specified`() {
        val consumer = mockk<KafkaConsumer<Any?, Any?>>(relaxed = true)
        val producer = mockk<KafkaProducer<Any?, Any?>>(relaxed = true)

        every { consumer.poll(any<Duration>()) } returnsMany listOf(
            ConsumerRecords(mapOf(TopicPartition("test", 0) to listOf(ConsumerRecord("test", 0, 0, mockk(), mockk())))),
            ConsumerRecords(emptyMap())
        )

        val pipeline = Pipeline(consumer, producer, false, true)

        pipeline.processTopic("test") {}

        verifyOrder {
            producer.flush()
            consumer.close()
        }

        verify(exactly = 0) {
            producer.sendOffsetsToTransaction(any(), any<ConsumerGroupMetadata>())
            consumer.commitSync()
        }
    }

    @Test
    fun `excludes offsets from transaction if no commit requested`() {
        val consumer = mockk<KafkaConsumer<Any?, Any?>>(relaxed = true)
        val producer = mockk<KafkaProducer<Any?, Any?>>(relaxed = true)

        every { consumer.poll(any<Duration>()) } returnsMany listOf(
            ConsumerRecords(mapOf(TopicPartition("test", 0) to listOf(ConsumerRecord("test", 0, 0, mockk(), mockk())))),
            ConsumerRecords(emptyMap())
        )

        val pipeline = Pipeline(consumer, producer, true, true)

        pipeline.processTopic("test") {}

        verifyOrder {
            producer.initTransactions()
            producer.beginTransaction()
            producer.commitTransaction()
        }

        verify(exactly = 0) {
            producer.sendOffsetsToTransaction(any(), any<ConsumerGroupMetadata>())
            consumer.commitSync()
        }
    }

    @Test
    fun `aborts transaction on exception and rethrows`() {
        val consumer = mockk<KafkaConsumer<Any?, Any?>>(relaxed = true)
        val producer = mockk<KafkaProducer<Any?, Any?>>(relaxed = true)

        every { consumer.poll(any<Duration>()) } returnsMany listOf(
            ConsumerRecords(mapOf(TopicPartition("test", 0) to listOf(ConsumerRecord("test", 0, 0, mockk(), mockk())))),
            ConsumerRecords(emptyMap())
        )

        val pipeline = Pipeline(consumer, producer, true, false)

        assertThrows<RuntimeException>("Blargh") {
            pipeline.processTopic("test") {
                throw RuntimeException("Blargh")
            }
        }

        verifyOrder {
            producer.initTransactions()
            producer.beginTransaction()
            producer.abortTransaction()
        }
    }

    @Test
    fun `can send record through callback arguments`() {
        val consumer = mockk<KafkaConsumer<Any?, Any?>>(relaxed = true)
        val producer = mockk<KafkaProducer<Any?, Any?>>(relaxed = true)

        every { consumer.poll(any<Duration>()) } returnsMany listOf(
            ConsumerRecords(mapOf(TopicPartition("test", 0) to listOf(ConsumerRecord("test", 0, 0, mockk(), mockk())))),
            ConsumerRecords(emptyMap())
        )

        val callbackSlot = slot<Callback>()
        every { producer.send(any(), capture(callbackSlot)) } answers {
            callbackSlot.captured.onCompletion(
                mockk(),
                null
            )

            mockk()
        }

        val record = ProducerRecord<Any?, Any?>("test", null, null)

        val pipeline = Pipeline(consumer, producer, false, false)

        pipeline.processTopic("test") {
            produce(record)
        }

        verify(exactly = 1) {
            producer.send(record, any())
        }
    }

    @Test
    fun `purging record writes tombstone record`() {
        val consumer = mockk<KafkaConsumer<Any?, Any?>>(relaxed = true)
        val producer = mockk<KafkaProducer<Any?, Any?>>(relaxed = true)

        every { consumer.poll(any<Duration>()) } returnsMany listOf(
            ConsumerRecords(mapOf(TopicPartition("test", 0) to listOf(ConsumerRecord("test", 42, 0, "key", mockk())))),
            ConsumerRecords(emptyMap())
        )

        val callbackSlot = slot<Callback>()
        every { producer.send(any(), capture(callbackSlot)) } answers {
            callbackSlot.captured.onCompletion(
                mockk(),
                null
            )

            mockk()
        }

        val pipeline = Pipeline(consumer, producer, false, false)

        pipeline.processTopic("test") {
            purge(it)
        }

        verify(exactly = 1) {
            producer.send(ProducerRecord("test", 42, "key", null), any())
        }
    }

    @Test
    fun `does not write anything when trying to purge tombstone record`() {
        val consumer = mockk<KafkaConsumer<Any?, Any?>>(relaxed = true)
        val producer = mockk<KafkaProducer<Any?, Any?>>(relaxed = true)

        every { consumer.poll(any<Duration>()) } returnsMany listOf(
            ConsumerRecords(mapOf(TopicPartition("test", 0) to listOf(ConsumerRecord("test", 42, 0, "key", null)))),
            ConsumerRecords(emptyMap())
        )

        val pipeline = Pipeline(consumer, producer, false, false)

        pipeline.processTopic("test") {
            purge(it)
        }

        verify(exactly = 0) {
            producer.send(any(), any())
        }
    }

    @Test
    fun `handles exception in producer sending`() {
        val consumer = mockk<KafkaConsumer<Any?, Any?>>(relaxed = true)
        val producer = mockk<KafkaProducer<Any?, Any?>>(relaxed = true)

        every { consumer.poll(any<Duration>()) } returnsMany listOf(
            ConsumerRecords(mapOf(TopicPartition("test", 0) to listOf(ConsumerRecord("test", 0, 0, mockk(), mockk())))),
            ConsumerRecords(emptyMap())
        )

        val callbackSlot = slot<Callback>()
        every { producer.send(any(), capture(callbackSlot)) } answers {
            callbackSlot.captured.onCompletion(
                mockk(),
                RuntimeException("Test message")
            )

            mockk()
        }


        val pipeline = Pipeline(consumer, producer, false, false)

        assertThrows<RuntimeException>("Test message") {
            pipeline.processTopic("test") {
                produce(mockk())
            }
        }
    }
}
