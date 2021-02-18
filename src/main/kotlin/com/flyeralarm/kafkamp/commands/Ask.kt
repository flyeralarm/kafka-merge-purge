package com.flyeralarm.kafkamp.commands

import com.flyeralarm.kafkamp.Pipeline
import com.flyeralarm.kafkamp.prettyPrint
import org.apache.kafka.clients.producer.ProducerRecord
import org.slf4j.Logger
import picocli.CommandLine
import java.util.concurrent.Callable

@CommandLine.Command(
    name = "ask",
    description = ["Asks for every record from the source topic whether it should be merged into the destination topic or simply purged"]
)
class Ask(
    private val logger: Logger,
    private val pipeline: Pipeline,
    private val actionSource: () -> Action
) : Callable<Int> {
    @CommandLine.Option(
        names = ["-T", "--merge-tombstones"],
        description = ["Merge tombstone records into destination topic as well"]
    )
    var mergeTombstones = false

    @CommandLine.Parameters(
        description = ["The topic to read (and purge) records from"]
    )
    lateinit var sourceTopic: String

    @CommandLine.Parameters(
        description = ["The topic to merge records into"]
    )
    lateinit var destinationTopic: String

    override fun call(): Int {
        logger.info("Reading topic '$sourceTopic'...")

        var merged = 0
        var purged = 0
        var skipped = 0

        try {
            pipeline.processTopic(sourceTopic) { record ->
                // Do not merge tombstone records unless specified
                if (record.value() == null && !mergeTombstones) {
                    logger.debug("Skipping tombstone record at offset #${record.offset()} in topic '${record.topic()}' (Partition #${record.partition()})")
                    return@processTopic
                }

                logger.info(
                    "Record at offset #${record.offset()} in topic '${record.topic()}' (Partition #${record.partition()}):\n" +
                        record.prettyPrint("    ")
                )

                when (actionSource()) {
                    Action.MERGE -> {
                        produce(ProducerRecord(destinationTopic, record.key(), record.value()))
                        purge(record)
                        logger.info("Merged record with key ${record.key()} at offset #${record.offset()} from source topic '${record.topic()}' (Partition #${record.partition()}) into '$destinationTopic'")
                        merged += 1
                    }
                    Action.PURGE -> {
                        purge(record)
                        logger.info("Purged record with key ${record.key()} at offset #${record.offset()} from topic '${record.topic()}' (Partition #${record.partition()})")
                        purged += 1
                    }
                    Action.SKIP -> {
                        logger.info("Skipping record")
                        skipped += 1
                    }
                }
            }
        } catch (exception: Exception) {
            logger.error("Failed to process all records from topic '$sourceTopic' into '$destinationTopic'", exception)
            logger.info("Successfully processed records: $merged merged, $purged purged, $skipped skipped")
            return 1
        }

        logger.info("Finished processing topic '$sourceTopic'")
        logger.info("Successfully processed records: $merged merged, $purged purged, $skipped skipped")

        return 0
    }

    enum class Action {
        MERGE, PURGE, SKIP
    }
}
