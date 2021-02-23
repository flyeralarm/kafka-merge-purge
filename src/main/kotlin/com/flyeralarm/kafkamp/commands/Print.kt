package com.flyeralarm.kafkamp.commands

import com.flyeralarm.kafkamp.MixedValue
import com.flyeralarm.kafkamp.prettyPrint
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.slf4j.Logger
import picocli.CommandLine
import java.time.Duration
import java.util.concurrent.Callable

@CommandLine.Command(
    name = "print",
    description = ["Prints all records from the specified topic. Does not commit offsets by default"]
)
class Print(
    private val logger: Logger,
    private val consumer: KafkaConsumer<MixedValue?, MixedValue?>,
    private val noCommit: Boolean
) : Callable<Int> {
    @CommandLine.Parameters(
        description = ["The topic to read from"]
    )
    lateinit var topic: String

    override fun call(): Int {
        logger.info("Reading topic '$topic'...")

        var printed = 0

        try {
            consumer.use {
                consumer.subscribe(listOf(topic))

                do {
                    val records = consumer.poll(Duration.ofSeconds(1))

                    for (record in records) {
                        logger.info(
                            "Record at offset #${record.offset()} in topic '${record.topic()}' (Partition #${record.partition()}):\n" +
                                record.prettyPrint("    ")
                        )
                        printed += 1
                    }

                    if (!noCommit) {
                        consumer.commitSync()
                    }
                } while (!records.isEmpty)
            }
        } catch (exception: Exception) {
            logger.error("Failed to print all records from topic '$topic'", exception)
            logger.info("$printed record(s) were printed")
            return 1
        }

        logger.info("Finished printing $printed record(s) from '$topic'")

        return 0
    }
}
