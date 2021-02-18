package com.flyeralarm.kafkamp

import org.apache.kafka.clients.consumer.ConsumerRecord
import kotlin.test.Test
import kotlin.test.assertEquals

class ConsumerRecordTest {
    @Test
    fun `can pretty print`() {
        assertEquals(
            """
            |    Key:
            |    KEY
            |    Value:
            |    VALUE
            """.trimMargin(),
            ConsumerRecord("test", 0, 0, "KEY", "VALUE").prettyPrint("    ")
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
            ConsumerRecord("test", 0, 0, null, "VALUE").prettyPrint("    ")
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
            ConsumerRecord("test", 0, 0, "KEY", null).prettyPrint("    ")
        )
    }
}
