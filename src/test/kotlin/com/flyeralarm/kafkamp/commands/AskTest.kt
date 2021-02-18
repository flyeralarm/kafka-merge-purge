package com.flyeralarm.kafkamp.commands

import com.flyeralarm.kafkamp.MixedValue
import com.flyeralarm.kafkamp.Pipeline
import com.flyeralarm.kafkamp.Record
import io.mockk.coVerify
import io.mockk.coVerifyOrder
import io.mockk.every
import io.mockk.mockk
import io.mockk.spyk
import io.mockk.verify
import io.mockk.verifyOrder
import kotlinx.coroutines.runBlocking
import org.apache.kafka.clients.producer.ProducerRecord
import org.slf4j.Logger
import kotlin.test.Test
import kotlin.test.assertEquals

class AskTest {
    @Test
    fun `produces records into destination topic and purges from source when action source yields MERGE`() {
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

        val actionSource = spyk({ Ask.Action.MERGE })
        val command = Ask(mockk(relaxed = true), pipeline, actionSource)
        command.sourceTopic = "source"
        command.destinationTopic = "destination"

        assertEquals(0, command.call())

        verify(exactly = 1) {
            pipeline.processTopic("source", any())
            actionSource()
        }

        coVerify {
            actions.produce(ProducerRecord("destination", key, value))
            actions.purge(record)
        }
    }

    @Test
    fun `purges record from source topic without merging if action source yields PURGE`() {
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

        val actionSource = spyk({ Ask.Action.PURGE })
        val command = Ask(mockk(relaxed = true), pipeline, actionSource)
        command.sourceTopic = "source"
        command.destinationTopic = "destination"

        assertEquals(0, command.call())

        verify(exactly = 1) {
            pipeline.processTopic("source", any())
            actionSource()
        }

        coVerify {
            actions.purge(record)
        }

        coVerify(exactly = 0) {
            actions.produce(any())
        }
    }

    @Test
    fun `does nothing with record if action source yields SKIP`() {
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

        val actionSource = spyk({ Ask.Action.SKIP })
        val command = Ask(mockk(relaxed = true), pipeline, actionSource)
        command.sourceTopic = "source"
        command.destinationTopic = "destination"

        assertEquals(0, command.call())

        verify(exactly = 1) {
            pipeline.processTopic("source", any())
            actionSource()
        }

        coVerify(exactly = 0) {
            actions.purge(record)
            actions.produce(any())
        }
    }

    @Test
    fun `logs statistics for processed records`() {
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
                actions.callback(record)
            }
        }

        var call = 0
        val actionSource = spyk({
            call += 1
            when (call) {
                1 -> Ask.Action.MERGE
                2 -> Ask.Action.PURGE
                else -> Ask.Action.SKIP
            }
        })
        val logger = mockk<Logger>(relaxed = true)
        val command = Ask(logger, pipeline, actionSource)
        command.sourceTopic = "source"
        command.destinationTopic = "destination"

        assertEquals(0, command.call())

        verify(exactly = 1) {
            pipeline.processTopic("source", any())
        }

        verify(exactly = 4) {
            actionSource()
        }

        coVerifyOrder {
            logger.info("Finished processing topic 'source'")
            logger.info("Successfully processed records: 1 merged, 1 purged, 2 skipped")
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

        val command = Ask(mockk(relaxed = true), pipeline) { Ask.Action.PURGE }
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

        val command = Ask(mockk(relaxed = true), pipeline) { Ask.Action.PURGE }
        command.sourceTopic = "source"
        command.destinationTopic = "destination"
        command.mergeTombstones = true

        assertEquals(0, command.call())

        coVerify(exactly = 1) {
            actions.purge(tombstoneRecord)
        }
    }

    @Test
    fun `exits with code 1 if pipeline throws exception and logs processed statistics`() {
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
        val command = Ask(logger, pipeline) { Ask.Action.MERGE }
        command.sourceTopic = "source"
        command.destinationTopic = "destination"

        assertEquals(1, command.call())

        verifyOrder {
            logger.error("Failed to process all records from topic 'source' into 'destination'", exception)
            logger.info("Successfully processed records: 1 merged, 0 purged, 0 skipped")
        }
    }
}
