package com.flyeralarm.kafkamp

import kotlinx.coroutines.runBlocking
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.clients.consumer.OffsetAndMetadata
import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.TopicPartition
import java.time.Duration
import kotlin.coroutines.resume
import kotlin.coroutines.resumeWithException
import kotlin.coroutines.suspendCoroutine

class Pipeline(
    private val consumer: KafkaConsumer<ByteArray?, ByteArray?>,
    private val producer: KafkaProducer<ByteArray?, ByteArray?>,
    private val recordDeserializer: RecordDeserializer,
    private val transactions: Boolean,
    private val noCommit: Boolean
) {
    private val actions = Actions()

    init {
        if (transactions) {
            producer.initTransactions()
        }
    }

    fun processTopic(
        topic: String,
        consume: suspend Actions.(record: RecordDeserializer.Record) -> Unit
    ) {
        consumer.use {
            consumer.subscribe(listOf(topic))

            try {
                generateSequence { consumer.poll(Duration.ofSeconds(1)) }
                    .takeWhile { !it.isEmpty }
                    .forEach { records ->
                        if (transactions) {
                            producer.beginTransaction()
                        }

                        val offsets = mutableMapOf<TopicPartition, OffsetAndMetadata>()

                        for (record in records) {
                            val deserialized = recordDeserializer.deserialize(record)

                            runBlocking {
                                actions.consume(deserialized)
                            }

                            offsets[TopicPartition(record.topic(), record.partition())] =
                                OffsetAndMetadata(record.offset())
                        }

                        if (transactions) {
                            if (!noCommit) {
                                producer.sendOffsetsToTransaction(offsets, consumer.groupMetadata())
                            }

                            producer.commitTransaction()
                        } else {
                            producer.flush()

                            if (!noCommit) {
                                consumer.commitSync()
                            }
                        }
                    }
            } catch (exception: Exception) {
                if (transactions) {
                    producer.abortTransaction()
                }

                throw exception
            }
        }
    }

    inner class Actions {
        suspend fun produce(record: ProducerRecord<ByteArray?, ByteArray?>) {
            suspendCoroutine<Unit> {
                producer.send(record) { _, exception ->
                    if (exception != null) {
                        it.resumeWithException(exception)
                    } else {
                        it.resume(Unit)
                    }
                }
            }
        }

        suspend fun purge(record: RecordDeserializer.Record) {
            // Tombstone records are already marked for deletion, so do not purge them again
            if (record.value == null) {
                return
            }

            produce(ProducerRecord(record.original.topic(), record.original.partition(), record.original.key(), null))
        }
    }
}
