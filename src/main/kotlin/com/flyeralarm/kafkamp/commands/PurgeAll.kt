package com.flyeralarm.kafkamp.commands

import com.flyeralarm.kafkamp.Pipeline
import org.slf4j.Logger
import picocli.CommandLine
import java.util.concurrent.Callable

@CommandLine.Command(
    name = "purge-all",
    description = ["Purges (i.e. writes a tombstone record for) every record from the specified topic"]
)
class PurgeAll(private val logger: Logger, private val pipeline: Pipeline) : Callable<Int> {
    @CommandLine.Parameters(
        description = ["The topic to purge"]
    )
    lateinit var topic: String

    override fun call(): Int {
        logger.info("Purging records in topic '$topic'...")

        var purged = 0
        try {
            pipeline.processTopic(topic) { record ->
                val serialized = record.original

                // Skip tombstone records, they're already purged
                if (record.value == null) {
                    logger.debug("Skipping tombstone record at offset #${serialized.offset()} in topic '${serialized.topic()}' (Partition #${serialized.partition()})")
                    return@processTopic
                }

                logger.debug(
                    "Purging record at offset #${serialized.offset()} in topic '${serialized.topic()}' (Partition #${serialized.partition()}):\n" +
                        record.prettyPrint("    ")
                )
                purge(record)
                logger.info("Purged record with key ${record.key} at offset #${serialized.offset()} from topic '${serialized.topic()}' (Partition #${serialized.partition()})")
                purged += 1
            }
        } catch (exception: Exception) {
            logger.error("Failed to purge all records from topic '$topic'", exception)
            logger.info("Successfully purged $purged record(s)")
            return 1
        }

        logger.info("Finished purging $purged record(s) from '$topic'")

        return 0
    }
}
