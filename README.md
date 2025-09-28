# Flink Iceberg Integration Project

A comprehensive integration test demonstrating streaming data from Kafka to Apache Iceberg using Apache Flink DataStream API.

## Overview

This project implements a complete **end-to-end data pipeline**: **Kafka Producer → Kafka → Flink DataStream → Iceberg Table** with embedded Kafka for testing and comprehensive data verification.

The test automatically generates and sends messages to Kafka, then processes them through Flink to Iceberg, providing a full integration validation.

## Architecture

```
┌─────────────┐    ┌─────────────┐    ┌──────────────┐    ┌─────────────┐
│   Producer  │───▶│   Kafka     │───▶│ Flink Stream │───▶│   Iceberg   │
│ (Test Data) │    │ (Embedded)  │    │   Processing │    │   Table     │
└─────────────┘    └─────────────┘    └──────────────┘    └─────────────┘
```

**End-to-End Flow:**
1. **Producer** generates 5 test messages automatically
2. **Kafka** stores messages in embedded broker
3. **Flink** reads and processes messages via DataStream API
4. **Iceberg** stores processed data with full verification

## Project Structure

### Core Components

- **`IcebergKafkaTest.java`** - Main integration test with embedded Kafka
- **`IcebergTableManager.java`** - Iceberg table creation and schema management
- **`DataTransformer.java`** - Data format transformations (Kafka → Avro → RowData)
- **`IcebergDataVerifier.java`** - Data integrity verification and validation
- **`FlinkKafkaConsumer.java`** - Kafka consumer for Flink streaming
- **`KafkaMessage.java`** - Simple message model for testing

### Test Flow

1. **Setup**: Embedded Kafka broker starts automatically
2. **Data Generation**: Test producer automatically generates and sends 5 messages to Kafka topic
3. **Stream Processing**: Flink reads from Kafka and transforms data using DataStream API
4. **Iceberg Write**: Processed data written to Iceberg table with checkpointing
5. **Verification**: Comprehensive validation of written data, snapshots, and file counts

**Key Integration Points:**
- ✅ **Producer Integration**: Automatic message generation and Kafka publishing
- ✅ **Kafka Integration**: Embedded broker with real message storage
- ✅ **Flink Integration**: DataStream API processing with checkpointing
- ✅ **Iceberg Integration**: Table creation, data writing, and snapshot validation

## Requirements

### System Requirements
- **Java 17+** (OpenJDK recommended)
- **Gradle 8+**
- **macOS/Linux** (tested on macOS)

### Dependencies
- **Apache Flink 1.19.1** - Stream processing framework
- **Apache Iceberg 1.9.1** - Table format for analytics
- **Apache Kafka** - Message streaming (embedded for testing)
- **Spring Kafka Test** - Embedded Kafka broker
- **Hadoop** - File system support for Iceberg

## Quick Start

### 1. Run the Integration Test

```bash
# Set Java 17 (required for Flink compatibility)
export JAVA_HOME=/opt/homebrew/opt/openjdk@17

# Run the complete end-to-end pipeline test
# This will automatically:
# - Start embedded Kafka broker
# - Generate and send 5 test messages to Kafka
# - Process messages through Flink DataStream API
# - Write data to Iceberg table
# - Verify data integrity and snapshots
./gradlew test --tests "org.example.IcebergKafkaTest.shouldWriteToIcebergFromKafka" --info
```

### 2. Expected Output

```
✅ Data successfully written to Iceberg table!
📊 Table location: build/iceberg-kafka-output/default_database/kafka_messages
📈 Total records: 5
📁 Data files: 1
💾 File size: 2239 bytes
🎯 Snapshot ID: 7438972446048711808
```

### 3. Run All Tests

```bash
./gradlew test
```

## Technical Details

### Data Schema

The pipeline processes messages with the following schema:

| Field     | Type    | Description           |
|-----------|---------|-----------------------|
| id        | String  | Unique message ID     |
| content   | String  | Message content       |
| timestamp | Long    | Processing timestamp  |
| topic     | String  | Kafka topic name      |
| partition | Integer | Kafka partition       |
| offset    | Long    | Kafka offset          |


### Performance Characteristics

- **Processing Time**: ~15-20 seconds for 5 messages
- **Data Size**: ~2.2KB compressed per test run
- **Checkpointing**: 5-second intervals for reliability

## Development

### Project Structure

```
src/
└── test/java/org/example/
    ├── IcebergKafkaTest.java        # Main integration test
    ├── IcebergTableManager.java     # Table management
    ├── DataTransformer.java         # Data transformations
    ├── IcebergDataVerifier.java     # Data verification
    ├── FlinkKafkaConsumer.java      # Kafka consumer
    └── KafkaMessage.java            # Message model
```

### Building

```bash
# Run tests with detailed output
./gradlew test --info
```

## Troubleshooting

### Common Issues

1. **Java Version**: Ensure Java 17+ is used
   ```bash
   export JAVA_HOME=/opt/homebrew/opt/openjdk@17
   ```

2. **Port Conflicts**: Embedded Kafka uses random ports, conflicts are rare

3. **Memory Issues**: Increase heap size if needed
   ```bash
   ./gradlew test -Dorg.gradle.jvmargs="-Xmx2g"
   ```

4. **Timeout Issues**: Test has 10-second timeout, increase if needed

### Verification

After running tests, check the Iceberg table:

```bash
# Check if data was written
ls -la build/iceberg-kafka-output/default_database/kafka_messages/

# View table metadata
find build/iceberg-kafka-output -name "*.avro" -o -name "*.parquet" -o -name "*.json"
```

## Contributing

1. Follow Java coding standards
2. Add JavaDoc for all public methods
3. Write tests for new functionality
4. Update README for significant changes

## License

**на всё воля божья - используйте как хотите!** 

This project is open source and free to use for any purpose - educational, commercial, or personal. No restrictions, no warranties, just pure freedom to explore and learn.

---

# 🔬 **TECHNICAL DEEP DIVE: SPARK VS FLINK STREAMING OPERATIONS**

## Key Difference: Manifest File Management

### Spark vs Flink Approach

- **⚡ Spark**: Automatically selects between FastAppend and MergeAppend based on manifest count thresholds and operation context
- **🔄 Flink**: Primarily uses FastAppend, requiring manual monitoring and periodic compaction to prevent manifest file bloat

### Critical Consideration for Flink

Since Flink doesn't automatically manage manifest files like Spark does, it's **most critical** to monitor and manage manifest file 

### Implementation References

- **[FastAppend.java](https://github.com/apache/iceberg/blob/main/core/src/main/java/org/apache/iceberg/FastAppend.java#L36)** - Optimized for fast data appending without manifest merging
- **[MergeAppend.java](https://github.com/apache/iceberg/blob/main/core/src/main/java/org/apache/iceberg/MergeAppend.java#L24)** - Performs manifest merging during write operations


