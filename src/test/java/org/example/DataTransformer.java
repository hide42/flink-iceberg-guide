package org.example;

import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.VarCharType;
import org.apache.flink.table.types.logical.BigIntType;
import org.apache.flink.table.types.logical.IntType;

/**
 * Transforms data between Kafka, Avro, and RowData formats.
 * Provides utilities for converting between different data representations used in the pipeline.
 */
public class DataTransformer {
    
    /**
     * RowType schema definition for Iceberg table.
     * Defines the structure: id, content, timestamp, topic, partition, offset.
     */
    public static final RowType ROW_TYPE = RowType.of(
        new LogicalType[]{
            VarCharType.STRING_TYPE,
            VarCharType.STRING_TYPE,
            new BigIntType(),
            VarCharType.STRING_TYPE,
            new IntType(),
            new BigIntType()
        },
        new String[]{"id", "content", "timestamp", "topic", "partition", "offset"}
    );
    
    /**
     * Converts a string record from Kafka into a KafkaMessage object.
     * Generates unique message ID and sets current timestamp.
     *
     * @param record the raw string record from Kafka
     * @param topicName the Kafka topic name
     * @return the constructed KafkaMessage
     */
    public static KafkaMessage stringToKafkaMessage(String record, String topicName) {
        KafkaMessage message = new KafkaMessage();
        message.setId("msg-" + System.currentTimeMillis());
        message.setContent(record);
        message.setTimestamp(System.currentTimeMillis());
        message.setTopic(topicName);
        message.setPartition(0);
        message.setOffset(0L);
        return message;
    }
    
    /**
     * Converts a KafkaMessage into RowData format for Iceberg sink.
     * Maps all fields according to the ROW_TYPE schema definition.
     *
     * @param message the KafkaMessage to convert
     * @return the RowData representation for Iceberg
     */
    public static RowData kafkaMessageToRowData(KafkaMessage message) {
        GenericRowData rowData = new GenericRowData(ROW_TYPE.getFieldCount());
        rowData.setField(0, StringData.fromString(message.getId()));
        rowData.setField(1, StringData.fromString(message.getContent()));
        rowData.setField(2, message.getTimestamp());
        rowData.setField(3, StringData.fromString(message.getTopic()));
        rowData.setField(4, message.getPartition());
        rowData.setField(5, message.getOffset());
        return rowData;
    }
}
