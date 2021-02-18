package com.flyeralarm.kafkamp

import org.slf4j.event.Level
import picocli.CommandLine
import java.io.InputStreamReader
import java.nio.file.Path

@CommandLine.Command(
    name = "kafka-merge-purge",
    mixinStandardHelpOptions = true,
    versionProvider = CLI.Companion::class,
    description = ["Merges Kafka records from one topic into another, marking them as deleted in the old topic in the process"],
    synopsisSubcommandLabel = "(ask | merge-all | purge-all | print)",
    usageHelpAutoWidth = true
)
class CLI {
    @CommandLine.Option(
        names = ["-g", "--group"],
        description = ["Consumer group for ingesting the source topic"],
        required = true,
        scope = CommandLine.ScopeType.INHERIT
    )
    lateinit var consumerGroup: String

    @CommandLine.Option(
        names = ["-b", "--bootstrap-servers"],
        description = ["Kafka Bootstrap servers as comma-delimited list. Takes precedence over properties files."],
        scope = CommandLine.ScopeType.INHERIT
    )
    var bootstrapServers: String? = null

    @CommandLine.Option(
        names = ["-P", "--properties"],
        description = ["A Java Properties file for shared client configuration (optional)"],
        scope = CommandLine.ScopeType.INHERIT
    )
    var propertiesFilePath: Path? = null

    @CommandLine.Option(
        names = ["--consumer-properties"],
        description = ["A Java Properties file for consumer client configuration (optional)"],
        scope = CommandLine.ScopeType.INHERIT
    )
    var consumerPropertiesFilePath: Path? = null

    @CommandLine.Option(
        names = ["--producer-properties"],
        description = ["A Java Properties file for consumer client configuration (optional)"],
        scope = CommandLine.ScopeType.INHERIT
    )
    var producerPropertiesFilePath: Path? = null

    @CommandLine.Option(
        names = ["-p", "--property"],
        description = ["Specify a shared client configuration property directly, may be used multiple times. These options takes precedence over properties files."],
        scope = CommandLine.ScopeType.INHERIT
    )
    var additionalProperties = mapOf<String, String>()

    @CommandLine.Option(
        names = ["-cp", "--consumer-property"],
        description = ["Specify a producer client configuration property directly, may be used multiple times. These options takes precedence over properties files."],
        scope = CommandLine.ScopeType.INHERIT
    )
    var additionalConsumerProperties = mapOf<String, String>()

    @CommandLine.Option(
        names = ["-pp", "--producer-property"],
        description = ["Specify a producer client configuration property directly, may be used multiple times. These options takes precedence over properties files."],
        scope = CommandLine.ScopeType.INHERIT
    )
    var additionalProducerProperties = mapOf<String, String>()

    @CommandLine.ArgGroup(
        exclusive = true,
        heading = "Key (de)serializer\n"
    )
    var keySerializer = DefaultKeySerializer()

    @CommandLine.ArgGroup(
        exclusive = true,
        heading = "Value (de)serializer\n"
    )
    var valueSerializer = DefaultValueSerializer()

    @CommandLine.Option(
        names = ["-t", "--transaction"],
        description = ["Produce records within a transaction. Optional value is the transactional ID to use. Defaults to a random UUID"],
        arity = "0..1",
        scope = CommandLine.ScopeType.INHERIT
    )
    var transactionalId: String? = null

    @CommandLine.Option(
        names = ["-n", "--no-commit"],
        description = ["Do not commit consumer offsets"],
        arity = "0..1",
        scope = CommandLine.ScopeType.INHERIT
    )
    var noCommit = false

    @CommandLine.Option(
        names = ["-v", "--verbose"],
        description = ["Enable verbose logging"],
        arity = "0..1",
        scope = CommandLine.ScopeType.INHERIT
    )
    fun setVerbose(verbose: Boolean) {
        if (verbose) {
            System.setProperty("log4j.root.level", Level.DEBUG.name)
            System.setProperty("log4j.cli.level", Level.DEBUG.name)
        }
    }

    class DefaultKeySerializer {
        @CommandLine.Option(
            names = ["-AK", "--avro-key"],
            description = ["Force Avro (de)serializer for record keys"],
            arity = "0..1",
            scope = CommandLine.ScopeType.INHERIT
        )
        var avro = false

        @CommandLine.Option(
            names = ["-SK", "--string-key"],
            description = ["Force String (de)serializer for record keys"],
            arity = "0..1",
            scope = CommandLine.ScopeType.INHERIT
        )
        var string = false
    }

    class DefaultValueSerializer {
        @CommandLine.Option(
            names = ["-A", "--avro"],
            description = ["Force Avro (de)serializer for record values"],
            arity = "0..1",
            scope = CommandLine.ScopeType.INHERIT
        )
        var avro = false

        @CommandLine.Option(
            names = ["-S", "--string"],
            description = ["Force String (de)serializer for record values"],
            arity = "0..1",
            scope = CommandLine.ScopeType.INHERIT
        )
        var string = false
    }

    companion object : CommandLine.IVersionProvider {
        override fun getVersion(): Array<String> {
            val version = InputStreamReader(
                this::class.java.classLoader.getResourceAsStream("version.txt")!!
            ).readText()

            return arrayOf("kafka-merge-purge ${version.trim()}")
        }
    }
}
