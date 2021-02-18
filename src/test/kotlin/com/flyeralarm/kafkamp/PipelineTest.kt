package com.flyeralarm.kafkamp

import com.flyeralarm.kafkamp.RecordDeserializer.Record
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
        val consumer = mockk<KafkaConsumer<ByteArray?, ByteArray?>>()
        val recordDeserializer = mockk<RecordDeserializer>(relaxed = true)

        every { consumer.subscribe(any<Collection<String>>()) } just runs
        every { consumer.commitSync() } just runs
        every { consumer.close() } just runs

        every { consumer.poll(any<Duration>()) } returnsMany listOf(
            ConsumerRecords(mapOf(TopicPartition("test", 0) to listOf(ConsumerRecord("test", 0, 0, byteArrayOf(), byteArrayOf())))),
            ConsumerRecords(mapOf(TopicPartition("test", 0) to listOf(ConsumerRecord("test", 0, 0, byteArrayOf(), byteArrayOf())))),
            ConsumerRecords(emptyMap())
        )

        val pipeline = Pipeline(consumer, mockk(relaxed = true), recordDeserializer, false, false)

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
    fun `calls callback for every deserialized record sequentially`() {
        val consumer = mockk<KafkaConsumer<ByteArray?, ByteArray?>>(relaxed = true)
        val producer = mockk<KafkaProducer<ByteArray?, ByteArray?>>(relaxed = true)
        val recordDeserializer = mockk<RecordDeserializer>(relaxed = true)

        val record1 = ConsumerRecord<ByteArray?, ByteArray?>("test", 0, 0, byteArrayOf(), byteArrayOf())
        val record2 = ConsumerRecord<ByteArray?, ByteArray?>("test", 0, 0, byteArrayOf(), byteArrayOf())
        val record3 = ConsumerRecord<ByteArray?, ByteArray?>("test", 0, 0, byteArrayOf(), byteArrayOf())

        every { consumer.poll(any<Duration>()) } returnsMany listOf(
            ConsumerRecords(mapOf(TopicPartition("test", 0) to listOf(record1, record2))),
            ConsumerRecords(mapOf(TopicPartition("test", 0) to listOf(record3))),
            ConsumerRecords(emptyMap())
        )

        val deserializedRecord1 = Record(record1, mockk(), mockk())
        val deserializedRecord2 = Record(record2, mockk(), mockk())
        val deserializedRecord3 = Record(record3, mockk(), mockk())

        every { recordDeserializer.deserialize(any()) } returnsMany listOf(
            deserializedRecord1,
            deserializedRecord2,
            deserializedRecord3
        )

        val pipeline = Pipeline(consumer, producer, recordDeserializer, false, false)

        val usedRecords = mutableListOf<Record>()
        pipeline.processTopic("test") {
            usedRecords += it
        }

        assertEquals(listOf(deserializedRecord1, deserializedRecord2, deserializedRecord3), usedRecords)

        verifyOrder {
            recordDeserializer.deserialize(record1)
            recordDeserializer.deserialize(record2)
            recordDeserializer.deserialize(record3)
        }
    }

    @Test
    fun `flushes producer after every iteration if not transactional`() {
        val consumer = mockk<KafkaConsumer<ByteArray?, ByteArray?>>(relaxed = true)
        val producer = mockk<KafkaProducer<ByteArray?, ByteArray?>>(relaxed = true)

        every { consumer.poll(any<Duration>()) } returnsMany listOf(
            ConsumerRecords(mapOf(TopicPartition("test", 0) to listOf(ConsumerRecord("test", 0, 0, byteArrayOf(), byteArrayOf())))),
            ConsumerRecords(emptyMap())
        )

        val pipeline = Pipeline(consumer, producer, mockk(relaxed = true), false, false)

        pipeline.processTopic("test") {}

        verifyOrder {
            producer.flush()
            consumer.commitSync()
        }
    }

    @Test
    fun `uses producer transactions when specified`() {
        val consumer = mockk<KafkaConsumer<ByteArray?, ByteArray?>>(relaxed = true)
        val producer = mockk<KafkaProducer<ByteArray?, ByteArray?>>(relaxed = true)

        every { consumer.poll(any<Duration>()) } returnsMany listOf(
            ConsumerRecords(mapOf(TopicPartition("test", 0) to listOf(ConsumerRecord("test", 0, 0, byteArrayOf(), byteArrayOf())))),
            ConsumerRecords(emptyMap())
        )

        val pipeline = Pipeline(consumer, producer, mockk(relaxed = true), true, false)

        pipeline.processTopic("test") {}

        verifyOrder {
            producer.initTransactions()
            producer.beginTransaction()
            producer.commitTransaction()
        }
    }

    @Test
    fun `commits offsets with transaction if using transaction`() {
        val consumer = mockk<KafkaConsumer<ByteArray?, ByteArray?>>(relaxed = true)
        val producer = mockk<KafkaProducer<ByteArray?, ByteArray?>>(relaxed = true)
        val consumerMetadata = mockk<ConsumerGroupMetadata>()

        every { consumer.groupMetadata() } returns consumerMetadata

        every { consumer.poll(any<Duration>()) } returnsMany listOf(
            ConsumerRecords(mapOf(TopicPartition("test", 0) to listOf(ConsumerRecord("test", 0, 0, byteArrayOf(), byteArrayOf())))),
            ConsumerRecords(emptyMap())
        )

        val pipeline = Pipeline(consumer, producer, mockk(relaxed = true), true, false)

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
        val consumer = mockk<KafkaConsumer<ByteArray?, ByteArray?>>(relaxed = true)
        val producer = mockk<KafkaProducer<ByteArray?, ByteArray?>>(relaxed = true)

        every { consumer.poll(any<Duration>()) } returnsMany listOf(
            ConsumerRecords(mapOf(TopicPartition("test", 0) to listOf(ConsumerRecord("test", 0, 0, byteArrayOf(), byteArrayOf())))),
            ConsumerRecords(emptyMap())
        )

        val pipeline = Pipeline(consumer, producer, mockk(relaxed = true), false, true)

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
        val consumer = mockk<KafkaConsumer<ByteArray?, ByteArray?>>(relaxed = true)
        val producer = mockk<KafkaProducer<ByteArray?, ByteArray?>>(relaxed = true)

        every { consumer.poll(any<Duration>()) } returnsMany listOf(
            ConsumerRecords(mapOf(TopicPartition("test", 0) to listOf(ConsumerRecord("test", 0, 0, byteArrayOf(), byteArrayOf())))),
            ConsumerRecords(emptyMap())
        )

        val pipeline = Pipeline(consumer, producer, mockk(relaxed = true), true, true)

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
        val consumer = mockk<KafkaConsumer<ByteArray?, ByteArray?>>(relaxed = true)
        val producer = mockk<KafkaProducer<ByteArray?, ByteArray?>>(relaxed = true)

        every { consumer.poll(any<Duration>()) } returnsMany listOf(
            ConsumerRecords(mapOf(TopicPartition("test", 0) to listOf(ConsumerRecord("test", 0, 0, byteArrayOf(), byteArrayOf())))),
            ConsumerRecords(emptyMap())
        )

        val pipeline = Pipeline(consumer, producer, mockk(relaxed = true), true, false)

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
        val consumer = mockk<KafkaConsumer<ByteArray?, ByteArray?>>(relaxed = true)
        val producer = mockk<KafkaProducer<ByteArray?, ByteArray?>>(relaxed = true)

        every { consumer.poll(any<Duration>()) } returnsMany listOf(
            ConsumerRecords(mapOf(TopicPartition("test", 0) to listOf(ConsumerRecord("test", 0, 0, byteArrayOf(), byteArrayOf())))),
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

        val record = ProducerRecord<ByteArray?, ByteArray?>("test", null, null)

        val pipeline = Pipeline(consumer, producer, mockk(relaxed = true), false, false)

        pipeline.processTopic("test") {
            produce(record)
        }

        verify(exactly = 1) {
            producer.send(record, any())
        }
    }

    @Test
    fun `purging record writes tombstone record`() {
        val consumer = mockk<KafkaConsumer<ByteArray?, ByteArray?>>(relaxed = true)
        val producer = mockk<KafkaProducer<ByteArray?, ByteArray?>>(relaxed = true)

        val key = byteArrayOf(42)
        val originalRecord = ConsumerRecord("test", 42, 0, key, byteArrayOf())
        val recordDeserializer = mockk<RecordDeserializer>(relaxed = true)

        every { recordDeserializer.deserialize(any()) } returns Record(originalRecord, "key", "value")

        every { consumer.poll(any<Duration>()) } returnsMany listOf(
            ConsumerRecords(mapOf(TopicPartition("test", 0) to listOf(originalRecord))),
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

        val pipeline = Pipeline(consumer, producer, recordDeserializer, false, false)

        pipeline.processTopic("test") {
            purge(it)
        }

        verify(exactly = 1) {
            producer.send(ProducerRecord("test", 42, key, null), any())
        }
    }

    @Test
    fun `does not write anything when trying to purge tombstone record`() {
        val consumer = mockk<KafkaConsumer<ByteArray?, ByteArray?>>(relaxed = true)
        val producer = mockk<KafkaProducer<ByteArray?, ByteArray?>>(relaxed = true)
        val recordDeserializer = mockk<RecordDeserializer>(relaxed = true)

        every { consumer.poll(any<Duration>()) } returnsMany listOf(
            ConsumerRecords(
                mapOf(
                    TopicPartition("test", 0) to listOf(
                        ConsumerRecord(
                            "test",
                            42,
                            0,
                            byteArrayOf(42),
                            null
                        )
                    )
                )
            ),
            ConsumerRecords(emptyMap())
        )

        every { recordDeserializer.deserialize(any()) } returns Record(mockk(), null, null)

        val pipeline = Pipeline(consumer, producer, recordDeserializer, false, false)

        pipeline.processTopic("test") {
            purge(it)
        }

        verify(exactly = 0) {
            producer.send(any(), any())
        }
    }

    @Test
    fun `handles exception in producer sending`() {
        val consumer = mockk<KafkaConsumer<ByteArray?, ByteArray?>>(relaxed = true)
        val producer = mockk<KafkaProducer<ByteArray?, ByteArray?>>(relaxed = true)

        every { consumer.poll(any<Duration>()) } returnsMany listOf(
            ConsumerRecords(mapOf(TopicPartition("test", 0) to listOf(ConsumerRecord("test", 0, 0, byteArrayOf(), byteArrayOf())))),
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

        val pipeline = Pipeline(consumer, producer, mockk(relaxed = true), false, false)

        assertThrows<RuntimeException>("Test message") {
            pipeline.processTopic("test") {
                produce(mockk())
            }
        }
    }
}
