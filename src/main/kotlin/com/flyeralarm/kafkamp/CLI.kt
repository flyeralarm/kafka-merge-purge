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
    usageHelpAutoWidth = true,
    sortOptions = false,
    usageHelpWidth = 120
)
class CLI {
    @CommandLine.Option(
        names = ["-b", "--bootstrap-servers"],
        description = ["Kafka Bootstrap servers as comma-delimited list.", "Takes precedence over properties files."],
        scope = CommandLine.ScopeType.INHERIT,
        order = 0
    )
    var bootstrapServers: String? = null

    @CommandLine.Option(
        names = ["-g", "--group"],
        description = ["Consumer group for ingesting the source topic"],
        required = true,
        scope = CommandLine.ScopeType.INHERIT,
        order = 1
    )
    lateinit var consumerGroup: String

    @CommandLine.Option(
        names = ["-O", "--properties"],
        description = ["A Java Properties file for shared client configuration (optional)"],
        scope = CommandLine.ScopeType.INHERIT,
        order = 2
    )
    var propertiesFilePath: Path? = null

    @CommandLine.Option(
        names = ["-o", "--property"],
        description = [
            "Specify a shared client configuration property directly.",
            "May be used multiple times.",
            "These options takes precedence over properties files."
        ],
        scope = CommandLine.ScopeType.INHERIT,
        order = 3
    )
    var additionalProperties = mapOf<String, String>()

    @CommandLine.Option(
        names = ["-C", "--consumer-properties"],
        description = ["A Java Properties file for consumer client configuration (optional)"],
        scope = CommandLine.ScopeType.INHERIT,
        order = 4
    )
    var consumerPropertiesFilePath: Path? = null

    @CommandLine.Option(
        names = ["-c", "--consumer-property"],
        description = [
            "Specify a consumer client configuration property directly.",
            "May be used multiple times.",
            "These options takes precedence over properties files."
        ],
        scope = CommandLine.ScopeType.INHERIT,
        order = 5
    )
    var additionalConsumerProperties = mapOf<String, String>()

    @CommandLine.Option(
        names = ["-P", "--producer-properties"],
        description = ["A Java Properties file for consumer client configuration (optional)"],
        scope = CommandLine.ScopeType.INHERIT,
        order = 6
    )
    var producerPropertiesFilePath: Path? = null

    @CommandLine.Option(
        names = ["-p", "--producer-property"],
        description = [
            "Specify a producer client configuration property directly.",
            "May be used multiple times.",
            "These options takes precedence over properties files."
        ],
        scope = CommandLine.ScopeType.INHERIT,
        order = 7
    )
    var additionalProducerProperties = mapOf<String, String>()

    @CommandLine.Option(
        names = ["-t", "--transaction"],
        description = [
            "Produce records within a transaction.",
            "Optional value is the transactional ID to use.",
            "Defaults to a random UUID"
        ],
        arity = "0..1",
        scope = CommandLine.ScopeType.INHERIT,
        order = 8
    )
    var transactionalId: String? = null

    @CommandLine.Option(
        names = ["-n", "--no-commit"],
        description = ["Do not commit consumer offsets.", "Explicitly set to false to make print commit its offsets"],
        arity = "0..1",
        scope = CommandLine.ScopeType.INHERIT,
        order = 9
    )
    var noCommit: Boolean? = null

    @CommandLine.Option(
        names = ["-A", "--avro-key"],
        description = [
            "Force Avro deserializer for record keys.",
            "Requires schema.registry.url consumer property to be set"
        ],
        scope = CommandLine.ScopeType.INHERIT,
        order = 10
    )
    var avroKeyDeserializer = false

    @CommandLine.Option(
        names = ["-a", "--avro"],
        description = [
            "Force Avro deserializer for record values.",
            "Requires schema.registry.url consumer property to be set"
        ],
        scope = CommandLine.ScopeType.INHERIT,
        order = 11
    )
    var avroValueDeserializer = false

    @CommandLine.Option(
        names = ["-v", "--verbose"],
        description = ["Enable verbose logging"],
        scope = CommandLine.ScopeType.INHERIT,
        order = 12
    )
    fun setVerbose(verbose: Boolean) {
        if (verbose) {
            System.setProperty("log4j.root.level", Level.DEBUG.name)
            System.setProperty("log4j.cli.level", Level.DEBUG.name)
        }
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
