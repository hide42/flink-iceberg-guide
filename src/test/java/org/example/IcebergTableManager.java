package org.example;

import org.apache.iceberg.Schema;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.hadoop.HadoopCatalog;
import org.apache.iceberg.types.Types;
import org.apache.hadoop.conf.Configuration;

/**
 * Manages Iceberg tables - schema creation and table management.
 * Provides utilities for creating Kafka message schemas and managing table lifecycle.
 */
public class IcebergTableManager {
    
    private final HadoopCatalog catalog;
    private final Configuration hadoopConf;
    
    /**
     * Creates a new IcebergTableManager with the specified warehouse path.
     *
     * @param warehousePath the path to the Iceberg warehouse directory
     */
    public IcebergTableManager(String warehousePath) {
        this.hadoopConf = new Configuration();
        this.catalog = new HadoopCatalog(hadoopConf, warehousePath);
    }
    
    /**
     * Returns the Hadoop catalog instance.
     *
     * @return the Hadoop catalog
     */
    public HadoopCatalog getCatalog() {
        return catalog;
    }
    
    /**
     * Returns the Hadoop configuration.
     *
     * @return the Hadoop configuration
     */
    public Configuration getHadoopConf() {
        return hadoopConf;
    }
    
    /**
     * Creates a schema for Kafka messages with standard fields.
     * Schema includes: id, content, timestamp, topic, partition, offset.
     *
     * @return the Iceberg schema for Kafka messages
     */
    public Schema createKafkaMessageSchema() {
        return new Schema(
            Types.NestedField.required(1, "id", Types.StringType.get()),
            Types.NestedField.required(2, "content", Types.StringType.get()),
            Types.NestedField.required(3, "timestamp", Types.LongType.get()),
            Types.NestedField.required(4, "topic", Types.StringType.get()),
            Types.NestedField.required(5, "partition", Types.IntegerType.get()),
            Types.NestedField.required(6, "offset", Types.LongType.get())
        );
    }
    
    /**
     * Creates a table if it doesn't exist, using the Kafka message schema.
     *
     * @param database the database name
     * @param tableName the table name
     * @return the table identifier
     */
    public TableIdentifier createTableIfNotExists(String database, String tableName) {
        TableIdentifier tableId = TableIdentifier.of(database, tableName);
        
        if (!catalog.tableExists(tableId)) {
            Schema schema = createKafkaMessageSchema();
            catalog.createTable(tableId, schema);
            System.out.println("Created Iceberg table: " + tableId);
        }
        
        return tableId;
    }
    
    /**
     * Constructs the full table path from components.
     *
     * @param warehousePath the warehouse path
     * @param database the database name
     * @param tableName the table name
     * @return the full table path
     */
    public String getTablePath(String warehousePath, String database, String tableName) {
        return warehousePath + "/" + database + "/" + tableName;
    }
}
