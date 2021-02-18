package com.flyeralarm.kafkamp

import org.junit.After
import org.junit.Before
import org.junit.Test
import org.slf4j.event.Level
import picocli.CommandLine
import java.io.ByteArrayOutputStream
import java.io.PrintStream
import java.io.PrintWriter
import java.nio.file.Paths
import kotlin.test.assertEquals
import kotlin.test.assertTrue

class CLITest {
    private val out = ByteArrayOutputStream()
    private val oldOut: PrintStream = System.out

    @Before
    fun setupStreams() {
        out.reset()
        System.setOut(PrintStream(out))
    }

    @After
    fun restoreStreams() {
        System.setOut(oldOut)
    }

    @Test
    fun `can print version information`() {
        val exitCode = CommandLine(CLI()).execute("--version")

        assertEquals("kafka-merge-purge foobar", out.toString().trim())
        assertEquals(0, exitCode)
    }

    @Test
    fun `can get version`() {
        assertEquals("kafka-merge-purge foobar", CLI.version.joinToString(""))
    }

    @Test
    fun `can enable verbose logging`() {
        CommandLine(CLI()).setErr(PrintWriter(out)).execute("--verbose")

        assertEquals(Level.DEBUG.name, System.getProperty("log4j.root.level"))
        assertEquals(Level.DEBUG.name, System.getProperty("log4j.cli.level"))

        System.clearProperty("log4j.root.level")
        System.clearProperty("log4j.cli.level")
    }

    @Test
    fun `does not change log level when verbose option is explicitly false`() {
        CommandLine(CLI()).setErr(PrintWriter(out)).execute("--verbose=false")

        assertEquals(null, System.getProperty("log4j.root.level"))
        assertEquals(null, System.getProperty("log4j.cli.level"))
    }

    @Test
    fun `can set and get consumer group`() {
        val options = CLI()
        CommandLine(options)
            .setErr(PrintWriter(out))
            .parseArgs(
                "-g", "test"
            )

        assertEquals("test", options.consumerGroup)
    }

    @Test
    fun `requires consumer group argument`() {
        val options = CLI()

        assertThrows<CommandLine.MissingParameterException>("Missing required option: '--group=<consumerGroup>'") {
            CommandLine(options)
                .setErr(PrintWriter(out))
                .parseArgs(
                    "--properties", "./test.properties"
                )
        }
    }

    @Test
    fun `can set and get bootstrap servers`() {
        val options = CLI()
        CommandLine(options)
            .setErr(PrintWriter(out))
            .parseArgs(
                "-b", "server1:9092,server2:9092",
                "-g", "test"
            )

        assertEquals("server1:9092,server2:9092", options.bootstrapServers)
    }

    @Test
    fun `can set and get properties file path`() {
        val options = CLI()
        CommandLine(options)
            .setErr(PrintWriter(out))
            .parseArgs(
                "--properties", "./test.properties",
                "-g", "test"
            )

        assertEquals(Paths.get("./test.properties"), options.propertiesFilePath)
    }

    @Test
    fun `can set and get consumer properties file path`() {
        val options = CLI()
        CommandLine(options)
            .setErr(PrintWriter(out))
            .parseArgs(
                "--consumer-properties", "./test.properties",
                "-g", "test"
            )

        assertEquals(Paths.get("./test.properties"), options.consumerPropertiesFilePath)
    }

    @Test
    fun `can set and get producer properties file path`() {
        val options = CLI()
        CommandLine(options)
            .setErr(PrintWriter(out))
            .parseArgs(
                "--producer-properties", "./test.properties",
                "-g", "test"
            )

        assertEquals(Paths.get("./test.properties"), options.producerPropertiesFilePath)
    }

    @Test
    fun `can set and get additional shared properties`() {
        val options = CLI()
        CommandLine(options)
            .setErr(PrintWriter(out))
            .parseArgs(
                "-p", "a=b",
                "-p", "foo=bar",
                "-g", "test"
            )

        assertEquals(mapOf("a" to "b", "foo" to "bar"), options.additionalProperties)
        assertEquals(emptyMap(), options.additionalConsumerProperties)
        assertEquals(emptyMap(), options.additionalProducerProperties)
    }

    @Test
    fun `can set and get additional consumer properties`() {
        val options = CLI()
        CommandLine(options)
            .setErr(PrintWriter(out))
            .parseArgs(
                "-cp", "a=b",
                "-cp", "foo=bar",
                "-g", "test"
            )

        assertEquals(emptyMap(), options.additionalProperties)
        assertEquals(mapOf("a" to "b", "foo" to "bar"), options.additionalConsumerProperties)
        assertEquals(emptyMap(), options.additionalProducerProperties)
    }

    @Test
    fun `can set and get additional producer properties`() {
        val options = CLI()
        CommandLine(options)
            .setErr(PrintWriter(out))
            .parseArgs(
                "-pp", "a=b",
                "-pp", "foo=bar",
                "-g", "test"
            )

        assertEquals(emptyMap(), options.additionalProperties)
        assertEquals(emptyMap(), options.additionalConsumerProperties)
        assertEquals(mapOf("a" to "b", "foo" to "bar"), options.additionalProducerProperties)
    }

    @Test
    fun `can set and get Avro key (de)serializer`() {
        val options = CLI()
        CommandLine(options)
            .setErr(PrintWriter(out))
            .parseArgs(
                "--avro-key",
                "-g", "test"
            )

        assertTrue(options.keySerializer.avro)
    }

    @Test
    fun `can set and get String key (de)serializer`() {
        val options = CLI()
        CommandLine(options)
            .setErr(PrintWriter(out))
            .parseArgs(
                "--string-key",
                "-g", "test"
            )

        assertTrue(options.keySerializer.string)
    }

    @Test
    fun `String and Avro key (de)serializer options are mutually exclusives`() {
        val options = CLI()

        assertThrows<CommandLine.MutuallyExclusiveArgsException>("Error: --avro-key, --string-key are mutually exclusive (specify only one)") {
            CommandLine(options)
                .setErr(PrintWriter(out))
                .parseArgs(
                    "--string-key",
                    "--avro-key",
                    "-g", "test"
                )
        }
    }

    @Test
    fun `can set and get Avro value (de)serializer`() {
        val options = CLI()
        CommandLine(options)
            .setErr(PrintWriter(out))
            .parseArgs(
                "--avro",
                "-g", "test"
            )

        assertTrue(options.valueSerializer.avro)
    }

    @Test
    fun `can set and get String value (de)serializer`() {
        val options = CLI()
        CommandLine(options)
            .setErr(PrintWriter(out))
            .parseArgs(
                "--string",
                "-g", "test"
            )

        assertTrue(options.valueSerializer.string)
    }

    @Test
    fun `String and Avro value (de)serializer options are mutually exclusives`() {
        val options = CLI()

        assertThrows<CommandLine.MutuallyExclusiveArgsException>("Error: --avro, --string are mutually exclusive (specify only one)") {
            CommandLine(options)
                .setErr(PrintWriter(out))
                .parseArgs(
                    "--string",
                    "--avro",
                    "-g", "test"
                )
        }
    }

    @Test
    fun `can set and get transactional id`() {
        val options = CLI()
        CommandLine(options)
            .setErr(PrintWriter(out))
            .parseArgs(
                "-t", "transactions.are.good",
                "-g", "test"
            )

        assertEquals("transactions.are.good", options.transactionalId)
    }

    @Test
    fun `can specify transaction option as toggle`() {
        val options = CLI()
        CommandLine(options)
            .setErr(PrintWriter(out))
            .parseArgs(
                "-t",
                "-g", "test"
            )

        assertEquals("", options.transactionalId)
    }

    @Test
    fun `can set and get no commit option`() {
        val options = CLI()
        CommandLine(options)
            .setErr(PrintWriter(out))
            .parseArgs(
                "-n",
                "-g", "test"
            )

        assertEquals(true, options.noCommit)
    }
}
