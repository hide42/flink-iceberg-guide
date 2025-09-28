package org.example;

/**
 * Simple Kafka message representation for testing purposes.
 * Contains basic fields commonly used in Kafka message processing.
 */
public class KafkaMessage {
    
    private String id;
    private String content;
    private Long timestamp;
    private String topic;
    private Integer partition;
    private Long offset;

    /**
     * Default constructor.
     */
    public KafkaMessage() {
    }

    /**
     * Gets the message ID.
     *
     * @return the message ID
     */
    public String getId() {
        return id;
    }

    /**
     * Sets the message ID.
     *
     * @param id the message ID
     */
    public void setId(String id) {
        this.id = id;
    }

    /**
     * Gets the message content.
     *
     * @return the message content
     */
    public String getContent() {
        return content;
    }

    /**
     * Sets the message content.
     *
     * @param content the message content
     */
    public void setContent(String content) {
        this.content = content;
    }

    /**
     * Gets the message timestamp.
     *
     * @return the message timestamp
     */
    public Long getTimestamp() {
        return timestamp;
    }

    /**
     * Sets the message timestamp.
     *
     * @param timestamp the message timestamp
     */
    public void setTimestamp(Long timestamp) {
        this.timestamp = timestamp;
    }

    /**
     * Gets the topic name.
     *
     * @return the topic name
     */
    public String getTopic() {
        return topic;
    }

    /**
     * Sets the topic name.
     *
     * @param topic the topic name
     */
    public void setTopic(String topic) {
        this.topic = topic;
    }

    /**
     * Gets the partition number.
     *
     * @return the partition number
     */
    public Integer getPartition() {
        return partition;
    }

    /**
     * Sets the partition number.
     *
     * @param partition the partition number
     */
    public void setPartition(Integer partition) {
        this.partition = partition;
    }

    /**
     * Gets the offset.
     *
     * @return the offset
     */
    public Long getOffset() {
        return offset;
    }

    /**
     * Sets the offset.
     *
     * @param offset the offset
     */
    public void setOffset(Long offset) {
        this.offset = offset;
    }

    @Override
    public String toString() {
        return "KafkaMessage{" +
                "id='" + id + '\'' +
                ", content='" + content + '\'' +
                ", timestamp=" + timestamp +
                ", topic='" + topic + '\'' +
                ", partition=" + partition +
                ", offset=" + offset +
                '}';
    }
}
