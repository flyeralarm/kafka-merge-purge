package com.flyeralarm.kafkamp.commands

import com.flyeralarm.kafkamp.MixedValue
import com.flyeralarm.kafkamp.Pipeline
import com.flyeralarm.kafkamp.Record
import io.mockk.coVerify
import io.mockk.every
import io.mockk.mockk
import io.mockk.verify
import io.mockk.verifyOrder
import kotlinx.coroutines.runBlocking
import org.apache.kafka.clients.producer.ProducerRecord
import org.slf4j.Logger
import kotlin.test.Test
import kotlin.test.assertEquals

class MergeAllTest {
    @Test
    fun `produces records into destination topic and purges from source`() {
        val pipeline = mockk<Pipeline>(relaxed = true)
        val actions = mockk<Pipeline.Actions>(relaxed = true)
        val key = MixedValue(byteArrayOf(42), "key")
        val value = MixedValue(byteArrayOf(99), "value")
        val record = Record("source", 0, 0, key, value)

        every { pipeline.processTopic(any(), captureLambda()) } answers {
            val callback = this.lambda<suspend Pipeline.Actions.(record: Record) -> Unit>().captured

            runBlocking {
                actions.callback(record)
            }
        }

        val command = MergeAll(mockk(relaxed = true), pipeline)
        command.sourceTopic = "source"
        command.destinationTopic = "destination"

        assertEquals(0, command.call())

        verify(exactly = 1) {
            pipeline.processTopic("source", any())
        }

        coVerify {
            actions.produce(ProducerRecord("destination", key, value))
            actions.purge(record)
        }
    }

    @Test
    fun `logs number of successfully merged records`() {
        val pipeline = mockk<Pipeline>(relaxed = true)
        val actions = mockk<Pipeline.Actions>(relaxed = true)
        val key = MixedValue(byteArrayOf(42), "key")
        val value = MixedValue(byteArrayOf(99), "value")
        val record = Record("source", 0, 0, key, value)

        every { pipeline.processTopic(any(), captureLambda()) } answers {
            val callback = this.lambda<suspend Pipeline.Actions.(record: Record) -> Unit>().captured

            runBlocking {
                actions.callback(record)
                actions.callback(record)
                actions.callback(record)
            }
        }

        val logger = mockk<Logger>(relaxed = true)

        val command = MergeAll(logger, pipeline)
        command.sourceTopic = "source"
        command.destinationTopic = "destination"

        assertEquals(0, command.call())

        verify(exactly = 1) {
            logger.info("Finished merging 3 record(s) from 'source' into 'destination'")
        }
    }

    @Test
    fun `skips tombstone consumer records from merging by default`() {
        val pipeline = mockk<Pipeline>(relaxed = true)
        val actions = mockk<Pipeline.Actions>(relaxed = true)
        val key = MixedValue(byteArrayOf(42), "key")
        val value = MixedValue(byteArrayOf(99), "value")
        val record = Record("source", 0, 0, key, value)
        val tombstoneRecord = Record("source", 0, 0, key, null)

        every { pipeline.processTopic(any(), captureLambda()) } answers {
            val callback = this.lambda<suspend Pipeline.Actions.(record: Record) -> Unit>().captured

            runBlocking {
                actions.callback(record)
                actions.callback(tombstoneRecord)
                actions.callback(record)
            }
        }

        val logger = mockk<Logger>(relaxed = true)

        val command = MergeAll(logger, pipeline)
        command.sourceTopic = "source"
        command.destinationTopic = "destination"

        assertEquals(0, command.call())

        coVerify(exactly = 0) {
            actions.purge(tombstoneRecord)
        }
    }

    @Test
    fun `merges tombstone consumer records when specified`() {
        val pipeline = mockk<Pipeline>(relaxed = true)
        val actions = mockk<Pipeline.Actions>(relaxed = true)
        val key = MixedValue(byteArrayOf(42), "key")
        val value = MixedValue(byteArrayOf(99), "value")
        val record = Record("source", 0, 0, key, value)
        val tombstoneRecord = Record("source", 0, 0, key, null)

        every { pipeline.processTopic(any(), captureLambda()) } answers {
            val callback = this.lambda<suspend Pipeline.Actions.(record: Record) -> Unit>().captured

            runBlocking {
                actions.callback(record)
                actions.callback(tombstoneRecord)
                actions.callback(record)
            }
        }

        val logger = mockk<Logger>(relaxed = true)

        val command = MergeAll(logger, pipeline)
        command.sourceTopic = "source"
        command.destinationTopic = "destination"
        command.mergeTombstones = true

        assertEquals(0, command.call())

        coVerify(exactly = 1) {
            actions.purge(tombstoneRecord)
        }
    }

    @Test
    fun `exits with code 1 if pipeline throws exception and logs total merged`() {
        val pipeline = mockk<Pipeline>(relaxed = true)
        val actions = mockk<Pipeline.Actions>(relaxed = true)
        val key = MixedValue(byteArrayOf(42), "key")
        val value = MixedValue(byteArrayOf(99), "value")
        val record = Record("source", 0, 0, key, value)
        val exception = RuntimeException("test")

        every { pipeline.processTopic(any(), captureLambda()) } answers {
            val callback = this.lambda<suspend Pipeline.Actions.(record: Record) -> Unit>().captured

            runBlocking {
                actions.callback(record)
                throw exception
            }
        }

        val logger = mockk<Logger>(relaxed = true)

        val command = MergeAll(logger, pipeline)
        command.sourceTopic = "source"
        command.destinationTopic = "destination"

        assertEquals(1, command.call())

        verifyOrder {
            logger.error("Failed to merge all records from topic 'source' into 'destination'", exception)
            logger.info("Successfully merged 1 record(s)")
        }
    }
}
