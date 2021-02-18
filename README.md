# kafka-merge-purge

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
