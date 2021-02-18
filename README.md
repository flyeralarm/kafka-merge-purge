# kafka-merge-purge
[![build](https://github.com/flyeralarm/kafka-merge-purge/workflows/build-dev/badge.svg)](https://github.com/domnikl/schema-registry-gitops/actions)
[![Docker Pulls](https://img.shields.io/docker/pulls/flyeralarm/kafka-merge-purge)](https://hub.docker.com/repository/docker/flyeralarm/kafka-merge-purge)
[![License](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](LICENSE)

Pipe data between Kafka topics with ease and choose what to do for individual records.

## Overview
This command line utility allows you to easily merge one Kafka topic into another, marking records from the source topic
for the deletion in the process. Designed for use with dead letter queues, you may choose the action to take for every
message in the source topic.

![Source topic -> kafka-merge-purge -> Print record -> User input -> Destination Topic](diagram.svg)

The application uses sane defaults for both producers and consumers, which partially may be overwritten by the user.
Currently, these defaults include the idempotent mode and `all` acknowledgements for the producer as well as
the `read_committed` isolation level for the consumer.

## Usage
```bash
Usage: kafka-merge-purge [-hnvV] [-t[=<transactionalId>]] [-b=<bootstrapServers>]
                         [-C=<consumerPropertiesFilePath>] -g=<consumerGroup> [-O=<propertiesFilePath>]
                         [-P=<producerPropertiesFilePath>] [-c=<String=String>]...
                         [-o=<String=String>]... [-p=<String=String>]... [-A | -S] [-a | -s]
                         (ask | merge-all | purge-all | print)
Merges Kafka records from one topic into another, marking them as deleted in the old topic in the process
  -h, --help                Show this help message and exit.
  -V, --version             Print version information and exit.
  -b, --bootstrap-servers=<bootstrapServers>
                            Kafka Bootstrap servers as comma-delimited list.
                            Takes precedence over properties files.
  -g, --group=<consumerGroup>
                            Consumer group for ingesting the source topic
  -O, --properties=<propertiesFilePath>
                            A Java Properties file for shared client configuration (optional)
  -o, --property=<String=String>
                            Specify a shared client configuration property directly.
                            May be used multiple times.
                            These options takes precedence over properties files.
  -C, --consumer-properties=<consumerPropertiesFilePath>
                            A Java Properties file for consumer client configuration (optional)
  -c, --consumer-property=<String=String>
                            Specify a consumer client configuration property directly.
                            May be used multiple times.
                            These options takes precedence over properties files.
  -P, --producer-properties=<producerPropertiesFilePath>
                            A Java Properties file for consumer client configuration (optional)
  -p, --producer-property=<String=String>
                            Specify a producer client configuration property directly.
                            May be used multiple times.
                            These options takes precedence over properties files.
  -t, --transaction[=<transactionalId>]
                            Produce records within a transaction.
                            Optional value is the transactional ID to use.
                            Defaults to a random UUID
  -n, --no-commit           Do not commit consumer offsets
  -v, --verbose             Enable verbose logging
Key (de)serializer
  -A, --avro-key            Force Avro (de)serializer for record keys
  -S, --string-key          Force String (de)serializer for record keys
Value (de)serializer
  -a, --avro                Force Avro (de)serializer for record values
  -s, --string              Force String (de)serializer for record values
```

## Running in Docker
`kafka-merge-purge` is available through Docker Hub, so running it in a container is as easy as:

```bash
docker run -v "$(pwd)/":/data flyeralarm/kafka-merge-purge --properties /data/client.properties -g consumer-group ask sourceTopic destinationTopic
```

Please keep in mind that using a tagged release may be a good idea.

## Options
### Transactions
kafka-merge-purge supports transactions out of the box. Simply specify the `-t` option to enable the transactional producer.
You may optionally specify the transactional ID to be used as a parameter to the `-t` option or through a configuration property.
If no transactional ID is specified, a random UUID will be used.

### Record (de)serialization
Currently, kafka-merge-purge only provides native support for the [Apache Avro](https://avro.apache.org/) serialization format.
You may specify the `-A` and `-a` options to enable Avro (de)serialization for record keys and values respectively.
Note that you will have to provide the `schema.registry.url` configuration value as well in order for records to be (de)serialized according to their schema.

String (de)serialization may be enabled using the `-S` and `-s` options.
