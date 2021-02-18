package com.flyeralarm.kafkamp

import com.flyeralarm.kafkamp.RecordDeserializer.Record
import io.mockk.every
import io.mockk.mockk
import io.mockk.verify
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.serialization.Deserializer
import kotlin.test.Test
import kotlin.test.assertEquals

class RecordDeserializerTest {
    @Test
    fun `uses provides deserializers to deserialize record`() {
        val keyDeserializer = mockk<Deserializer<Any?>>(relaxed = true)
        val valueDeserializer = mockk<Deserializer<Any?>>(relaxed = true)

        every { keyDeserializer.deserialize(any(), any(), any()) } returns "key"
        every { valueDeserializer.deserialize(any(), any(), any()) } returns "value"

        val key = byteArrayOf(42)
        val value = byteArrayOf(99)
        val originalRecord = ConsumerRecord("test", 0, 0, key, value)

        val recordDeserializer = RecordDeserializer(keyDeserializer, valueDeserializer)
        assertEquals(
            Record(originalRecord, "key", "value"),
            recordDeserializer.deserialize(originalRecord)
        )

        verify(exactly = 1) {
            keyDeserializer.deserialize("test", originalRecord.headers(), key)
            valueDeserializer.deserialize("test", originalRecord.headers(), value)
        }
    }

    @Test
    fun `returns null for key if original key was null`() {
        val keyDeserializer = mockk<Deserializer<Any?>>(relaxed = true)
        val valueDeserializer = mockk<Deserializer<Any?>>(relaxed = true)

        every { valueDeserializer.deserialize(any(), any(), any()) } returns "value"

        val originalRecord = ConsumerRecord<ByteArray?, ByteArray?>("test", 0, 0, null, byteArrayOf(42))

        val recordDeserializer = RecordDeserializer(keyDeserializer, valueDeserializer)
        assertEquals(
            Record(originalRecord, null, "value"),
            recordDeserializer.deserialize(originalRecord)
        )

        verify(exactly = 0) {
            keyDeserializer.deserialize(any(), any(), any())
        }
    }

    @Test
    fun `returns null for value if original value was null`() {
        val keyDeserializer = mockk<Deserializer<Any?>>(relaxed = true)
        val valueDeserializer = mockk<Deserializer<Any?>>(relaxed = true)

        every { keyDeserializer.deserialize(any(), any(), any()) } returns "key"

        val originalRecord = ConsumerRecord<ByteArray?, ByteArray?>("test", 0, 0, byteArrayOf(42), null)

        val recordDeserializer = RecordDeserializer(keyDeserializer, valueDeserializer)
        assertEquals(
            Record(originalRecord, "key", null),
            recordDeserializer.deserialize(originalRecord)
        )

        verify(exactly = 0) {
            valueDeserializer.deserialize(any(), any(), any())
        }
    }

    class RecordTest {
        @Test
        fun `can pretty print`() {
            assertEquals(
                """
            |    Key:
            |    KEY
            |    Value:
            |    VALUE
            """.trimMargin(),
                Record(mockk(), "KEY", "VALUE").prettyPrint("    ")
            )
        }

        @Test
        fun `can pretty print null key`() {
            assertEquals(
                """
            |    Key:
            |    null
            |    Value:
            |    VALUE
            """.trimMargin(),
                Record(mockk(), null, "VALUE").prettyPrint("    ")
            )
        }

        @Test
        fun `can pretty print tombstone`() {
            assertEquals(
                """
            |    Key:
            |    KEY
            |    Value:
            |    <tombstone>
            """.trimMargin(),
                Record(mockk(), "KEY", null).prettyPrint("    ")
            )
        }
    }
}
