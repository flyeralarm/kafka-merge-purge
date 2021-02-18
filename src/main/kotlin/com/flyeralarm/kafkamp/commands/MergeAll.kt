package com.flyeralarm.kafkamp.commands

import com.flyeralarm.kafkamp.Pipeline
import org.apache.kafka.clients.producer.ProducerRecord
import org.slf4j.Logger
import picocli.CommandLine
import java.util.concurrent.Callable

@CommandLine.Command(
    name = "merge-all",
    description = ["Merges all records from the source topic into the specified destination topic and marks them for deletion in the source topic"]
)
class MergeAll(private val logger: Logger, private val pipeline: Pipeline) : Callable<Int> {
    @CommandLine.Option(
        names = ["-T", "--merge-tombstones"],
        description = ["Merge tombstone records into destination topic as well"]
    )
    var mergeTombstones = false

    @CommandLine.Parameters(
        description = ["The topic to read and purge records from"]
    )
    lateinit var sourceTopic: String

    @CommandLine.Parameters(
        description = ["The topic to merge records into"]
    )
    lateinit var destinationTopic: String

    override fun call(): Int {
        logger.info("Merging topic '$sourceTopic' into '$destinationTopic'...")

        var merged = 0
        try {
            pipeline.processTopic(sourceTopic) { record ->
                val serialized = record.original

                // Do not merge tombstone records unless specified
                if (record.value == null && !mergeTombstones) {
                    logger.debug("Skipping tombstone record at offset #${serialized.offset()} in topic '${serialized.topic()}' (Partition #${serialized.partition()})")
                    return@processTopic
                }

                logger.debug(
                    "Merging record at offset #${serialized.offset()} in topic '${serialized.topic()}' (Partition #${serialized.partition()}) into '$destinationTopic':\n" +
                        record.prettyPrint("    ")
                )
                produce(ProducerRecord(destinationTopic, serialized.key(), serialized.value()))
                purge(record)
                logger.info("Merged record with key ${record.key} at offset #${serialized.offset()} from source topic '${serialized.topic()}' (Partition #${serialized.partition()}) into '$destinationTopic'")
                merged += 1
            }
        } catch (exception: Exception) {
            logger.error("Failed to merge all records from topic '$sourceTopic' into '$destinationTopic'", exception)
            logger.info("Successfully merged $merged record(s)")
            return 1
        }

        logger.info("Finished merging $merged record(s) from '$sourceTopic' into '$destinationTopic'")

        return 0
    }
}
