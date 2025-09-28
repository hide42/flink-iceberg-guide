package org.example;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.test.EmbeddedKafkaBroker;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.table.data.RowData;
import org.apache.iceberg.flink.sink.FlinkSink;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.hadoop.HadoopCatalog;

import java.util.concurrent.CompletableFuture;
import java.util.Properties;

/**
 * Integration test for Flink streaming data from Kafka to Iceberg.
 * Uses embedded Kafka broker for end-to-end testing of the data pipeline.
 * Tests the complete flow: Kafka → Flink DataStream → Iceberg table.
 */
@EmbeddedKafka(
    partitions = 1,
    topics = "iceberg-test-topic",
    kraft = true
)
public class IcebergKafkaTest {
    
    private static final Logger logger = LoggerFactory.getLogger(IcebergKafkaTest.class);
    private static final String TOPIC_NAME = "iceberg-test-topic";
    
    private static KafkaProducer<String, String> producer;
    private static String bootstrapServers;
    
    
    /**
     * Sets up embedded Kafka broker and producer for testing.
     * Initializes Kafka producer and sends test messages to the topic.
     *
     */
    @BeforeAll
    static void setupEmbeddedKafka() {
        logger.info("Setting up embedded Kafka for Iceberg test...");
        
        bootstrapServers = System.getProperty("spring.embedded.kafka.brokers", "localhost:9092");
        logger.info("Bootstrap servers: {}", bootstrapServers);
        
        // Send messages to Kafka FIRST
        logger.info("Sending messages to Kafka...");
        
        // Create Kafka producer
        Properties producerProps = new Properties();
        producerProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        producerProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        producerProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        producerProps.put(ProducerConfig.ACKS_CONFIG, "all");
        producerProps.put(ProducerConfig.RETRIES_CONFIG, 3);
        
        producer = new KafkaProducer<>(producerProps);
        
        for (int i = 1; i <= 5; i++) {
            String message = "Iceberg Kafka message " + i;
            producer.send(new ProducerRecord<>(TOPIC_NAME, message));
            logger.info("Sent message: {}", message);
        }
        producer.flush();
        
        logger.info("Embedded Kafka setup completed!");
    }
    
    /**
     * Cleans up resources after all tests complete.
     * Closes Kafka producer and destroys embedded Kafka broker.
     */
    @AfterAll
    static void cleanup() {
        if (producer != null) {
            producer.close();
        }
    }

    /**
     * Clears the output directory for idempotent tests.
     * Removes all files and subdirectories, then creates the directory.
     *
     * @param outputPath the path to clear
     */
    void clearDir(String outputPath){
        java.io.File outputDir = new java.io.File(outputPath);
        if (outputDir.exists()) {
        try (var paths = java.nio.file.Files.walk(outputDir.toPath())) {
            paths.sorted(java.util.Comparator.reverseOrder())
                    .map(java.nio.file.Path::toFile)
                    .forEach(file -> {
                        if (!file.delete()) {
                            logger.warn("Failed to delete file: {}", file.getPath());
                        }
                    });
        } catch (java.io.IOException e) {
            logger.warn("Failed to clear directory: {}", e.getMessage());
        }
        }
        if (!outputDir.mkdirs()) {
            logger.warn("Failed to create directory: {}", outputDir.getPath());
        }
    }
    
    /**
     * Tests the complete pipeline: Kafka → Flink DataStream → Iceberg.
     * Verifies that data flows correctly from Kafka through Flink to Iceberg table.
     * 
     * @throws Exception if the test fails
     */
    @Test
    @org.junit.jupiter.api.DisplayName("Test writing to Iceberg from Kafka")
    void shouldWriteToIcebergFromKafka() throws Exception {

        String outputPath = "build/iceberg-kafka-output";
        this.clearDir(outputPath);
        
        FlinkKafkaConsumer flinkConsumer = new FlinkKafkaConsumer(bootstrapServers, TOPIC_NAME, "iceberg-kafka-test-group");
        
        logger.info("Starting Flink job to write to Iceberg...");
        
        IcebergTableManager tableManager = new IcebergTableManager(outputPath);
        String tablePath = tableManager.getTablePath(outputPath, "default_database", "kafka_messages");
        try (org.apache.iceberg.flink.TableLoader tableLoader = org.apache.iceberg.flink.TableLoader.fromHadoopTable(tablePath, tableManager.getHadoopConf())) {
        
        HadoopCatalog catalog = tableManager.getCatalog();
        TableIdentifier tableId = tableManager.createTableIfNotExists("default_database", "kafka_messages");

        CompletableFuture<Void> flinkJob = CompletableFuture.runAsync(() -> {
            try {
                StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
                env.setParallelism(1);
                env.enableCheckpointing(5000);

                DataStream<String> kafkaStream = flinkConsumer.createKafkaStream(env, OffsetsInitializer.earliest());
                DataStream<KafkaMessage> avroStream = kafkaStream.map(record -> {
                    logger.info("Processing Kafka record: {}", record);
                    KafkaMessage message = DataTransformer.stringToKafkaMessage(record, flinkConsumer.getTopicName());
                    logger.info("Created Avro message: {}", message);
                    return message;
                });
                
                DataStream<RowData> rowDataStream = avroStream.map(DataTransformer::kafkaMessageToRowData);
                
                FlinkSink.forRowData(rowDataStream)
                    .tableLoader(tableLoader)
                    .append();
                
                logger.info("Starting Flink job with DataStream API...");
                env.execute("Kafka to File Streaming Job");
                
                logger.info("Flink job completed successfully!");
            } catch (Exception e) {
                logger.error("Flink job failed: {}", e.getMessage());
                throw new RuntimeException("Flink job failed", e);
            }
        });
        
        try {
            flinkJob.get(10, java.util.concurrent.TimeUnit.SECONDS);
            logger.info("Flink job completed successfully!");
        } catch (java.util.concurrent.TimeoutException e) {
            logger.warn("Flink job timed out after 10 seconds, stopping...");
            flinkConsumer.stop();
            Thread.sleep(2000);
        } catch (Exception e) {
            logger.error("Flink job failed: {}", e.getMessage());
            flinkConsumer.stop();
            throw e;
        }
        
        IcebergDataVerifier.verifyDataWritten(catalog, tableId);
        logger.info("Iceberg Kafka test completed successfully!");
        }
    }
    
}
