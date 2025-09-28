package org.example;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.concurrent.CompletableFuture;

/**
 * Simple Kafka consumer for Flink streaming applications.
 * Provides utilities for creating Kafka data streams and managing job execution.
 */
public class FlinkKafkaConsumer implements java.io.Serializable {
    
    private final String bootstrapServers;
    private final String topicName;
    private final String groupId;

    /**
     * Creates a new FlinkKafkaConsumer with the specified configuration.
     *
     * @param bootstrapServers the Kafka bootstrap servers
     * @param topicName the topic name to consume from
     * @param groupId the consumer group ID
     */
    public FlinkKafkaConsumer(String bootstrapServers, String topicName, String groupId) {
        this.bootstrapServers = bootstrapServers;
        this.topicName = topicName;
        this.groupId = groupId;
    }

    /**
     * Creates a Kafka data stream using the specified execution environment and offset initializer.
     *
     * @param env the Flink execution environment
     * @param offsetsInitializer the offset initializer strategy
     * @return the Kafka data stream
     */
    public DataStream<String> createKafkaStream(StreamExecutionEnvironment env, OffsetsInitializer offsetsInitializer) {
        KafkaSource<String> source = KafkaSource.<String>builder()
            .setBootstrapServers(bootstrapServers)
            .setTopics(topicName)
            .setGroupId(groupId)
            .setStartingOffsets(offsetsInitializer)
            .setValueOnlyDeserializer(new SimpleStringSchema())
            .build();

        return env.fromSource(source, WatermarkStrategy.noWatermarks(), "Kafka Source");
    }

    /**
     * Returns the topic name this consumer is configured for.
     *
     * @return the topic name
     */
    public String getTopicName() {
        return topicName;
    }

    /**
     * Stops the running job if it's currently executing.
     */
    public void stop() {
        // Job stopping logic can be implemented here if needed
    }
}
