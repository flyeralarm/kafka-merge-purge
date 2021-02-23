package com.flyeralarm.kafkamp

import io.mockk.mockk
import org.apache.kafka.common.header.Headers
import org.apache.kafka.common.serialization.Deserializer
import org.apache.kafka.common.serialization.StringDeserializer
import kotlin.test.BeforeTest
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFalse
import kotlin.test.assertNotEquals
import kotlin.test.assertNotNull
import kotlin.test.assertNull
import kotlin.test.assertTrue

class MixedValueTest {
    @Test
    fun `equals matches value equals and ignores raw value`() {
        val value = Math.random()
        val a = MixedValue(byteArrayOf(42), value)
        val b = MixedValue(byteArrayOf(99), value)
        val c = MixedValue(byteArrayOf(42), -1)

        assertEquals(a, b)
        assertNotEquals(a, c)
    }

    @Test
    fun `equals returns false for values of different type`() {
        assertFalse(MixedValue(byteArrayOf(42), "test").equals("test"))
    }

    @Test
    fun `hashCode matches value hashCode and ignores raw value`() {
        val value = Math.random()
        val a = MixedValue(byteArrayOf(42), value)
        val b = MixedValue(byteArrayOf(99), value)

        assertEquals(value.hashCode(), a.hashCode())
        assertEquals(value.hashCode(), b.hashCode())
    }

    @Test
    fun `toString matches value toString and ignores raw value`() {
        val value = Math.random()
        val a = MixedValue(byteArrayOf(42), value)
        val b = MixedValue(byteArrayOf(99), value)

        assertEquals(value.toString(), a.toString())
        assertEquals(value.toString(), b.toString())
    }

    class DeserializerTest {
        @BeforeTest
        fun `reset mock`() {
            MockDeserializer.reset()
        }

        @Test
        fun `configures delegate`() {
            val config = mutableMapOf(
                MixedValue.Deserializer.KEY_DELEGATE_CONFIG to MockDeserializer::class.java,
                MixedValue.Deserializer.VALUE_DELEGATE_CONFIG to StringDeserializer::class.java,
                "random" to 123
            )

            val deserializer = MixedValue.Deserializer()
            deserializer.configure(config, true)

            assertTrue(MockDeserializer.configured)
        }

        @Test
        fun `uses key delegate when specified and keeps raw data`() {
            MockDeserializer.valueForRegularCall = "key"

            val deserializer = MixedValue.Deserializer()
            deserializer.configure(
                mutableMapOf(
                    MixedValue.Deserializer.KEY_DELEGATE_CONFIG to MockDeserializer::class.java,
                    MixedValue.Deserializer.VALUE_DELEGATE_CONFIG to StringDeserializer::class.java
                ),
                true
            )
            val raw = "test".toByteArray()
            val result = deserializer.deserialize("test", raw)

            assertNotNull(result)
            assertEquals(raw, result.raw)
            assertEquals("key", result.value)

            assertEquals(1, MockDeserializer.regularCalls)
        }

        @Test
        fun `uses value delegate when specified and keeps raw data`() {
            MockDeserializer.valueForRegularCall = "value"

            val deserializer = MixedValue.Deserializer()
            deserializer.configure(
                mutableMapOf(
                    MixedValue.Deserializer.KEY_DELEGATE_CONFIG to StringDeserializer::class.java,
                    MixedValue.Deserializer.VALUE_DELEGATE_CONFIG to MockDeserializer::class.java
                ),
                false
            )
            val raw = "test".toByteArray()
            val result = deserializer.deserialize("test", raw)

            assertNotNull(result)
            assertEquals(raw, result.raw)
            assertEquals("value", result.value)

            assertEquals(1, MockDeserializer.regularCalls)
        }

        @Test
        fun `returns null if raw data is null`() {
            val deserializer = MixedValue.Deserializer()
            deserializer.configure(
                mutableMapOf(
                    MixedValue.Deserializer.KEY_DELEGATE_CONFIG to StringDeserializer::class.java,
                    MixedValue.Deserializer.VALUE_DELEGATE_CONFIG to MockDeserializer::class.java
                ),
                false
            )

            assertNull(deserializer.deserialize("test", null))

            assertEquals(0, MockDeserializer.regularCalls)
            assertEquals(0, MockDeserializer.headersCalls)
        }

        @Test
        fun `returns null if delegate returns null`() {
            val deserializer = MixedValue.Deserializer()
            deserializer.configure(
                mutableMapOf(
                    MixedValue.Deserializer.KEY_DELEGATE_CONFIG to StringDeserializer::class.java,
                    MixedValue.Deserializer.VALUE_DELEGATE_CONFIG to MockDeserializer::class.java
                ),
                false
            )

            assertNull(deserializer.deserialize("test", byteArrayOf(42)))
        }

        @Test
        fun `uses delegate when using serialize method with headers`() {
            MockDeserializer.valueForHeadersCall = "calledWithHeaders"

            val deserializer = MixedValue.Deserializer()
            deserializer.configure(
                mutableMapOf(
                    MixedValue.Deserializer.KEY_DELEGATE_CONFIG to StringDeserializer::class.java,
                    MixedValue.Deserializer.VALUE_DELEGATE_CONFIG to MockDeserializer::class.java
                ),
                false
            )

            val headers = mockk<Headers>()

            val raw = "test".toByteArray()
            val result = deserializer.deserialize("test", headers, raw)

            assertNotNull(result)
            assertEquals(raw, result.raw)
            assertEquals("calledWithHeaders", result.value)

            assertEquals(0, MockDeserializer.regularCalls)
            assertEquals(1, MockDeserializer.headersCalls)
        }

        @Test
        fun `closes delegate`() {
            val deserializer = MixedValue.Deserializer()
            deserializer.configure(
                mutableMapOf(
                    MixedValue.Deserializer.KEY_DELEGATE_CONFIG to StringDeserializer::class.java,
                    MixedValue.Deserializer.VALUE_DELEGATE_CONFIG to MockDeserializer::class.java
                ),
                false
            )

            deserializer.close()

            assertTrue(MockDeserializer.closed)
        }

        @Test
        fun `can close without configuring`() {
            val deserializer = MixedValue.Deserializer()

            deserializer.close()
        }

        class MockDeserializer : Deserializer<Any?> {
            override fun configure(configs: MutableMap<String, *>?, isKey: Boolean) {
                configured = true
            }

            override fun deserialize(topic: String, data: ByteArray?): Any? {
                regularCalls += 1
                return valueForRegularCall
            }

            override fun deserialize(topic: String?, headers: Headers?, data: ByteArray?): Any? {
                headersCalls += 1
                return valueForHeadersCall
            }

            override fun close() {
                closed = true
            }

            companion object {
                var configured = false
                var valueForRegularCall: Any? = null
                var regularCalls = 0
                var valueForHeadersCall: Any? = null
                var headersCalls = 0
                var closed = false

                fun reset() {
                    configured = false
                    valueForRegularCall = null
                    regularCalls = 0
                    valueForHeadersCall = null
                    headersCalls = 0
                    closed = false
                }
            }
        }
    }

    class SerializerTest {
        @Test
        fun `always returns raw value`() {
            val serializer = MixedValue.Serializer()
            val raw = byteArrayOf(42, 99)

            assertEquals(raw, serializer.serialize("topic", MixedValue(raw, "test")))
        }

        @Test
        fun `returns null for null value`() {
            val serializer = MixedValue.Serializer()

            assertNull(serializer.serialize("topic", null))
        }
    }
}
