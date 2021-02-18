package com.flyeralarm.kafkamp

import kotlinx.coroutines.runBlocking
import org.apache.kafka.clients.consumer.ConsumerRecord
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
    private val consumer: KafkaConsumer<Any?, Any?>,
    private val producer: KafkaProducer<Any?, Any?>,
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
        consume: suspend Actions.(record: ConsumerRecord<Any?, Any?>) -> Unit
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
                            runBlocking {
                                actions.consume(record)
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
        suspend fun produce(record: ProducerRecord<Any?, Any?>) {
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

        suspend fun purge(record: ConsumerRecord<Any?, Any?>) {
            // Tombstone records are already marked for deletion, so do not purge them again
            if (record.value() == null) {
                return
            }

            produce(ProducerRecord(record.topic(), record.partition(), record.key(), null))
        }
    }
}
