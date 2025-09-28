package org.example;

import org.apache.iceberg.Snapshot;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.hadoop.HadoopCatalog;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Verifies data integrity in Iceberg tables.
 * Provides utilities for checking that data was successfully written and validating table state.
 */
public class IcebergDataVerifier {
    
    private static final Logger logger = LoggerFactory.getLogger(IcebergDataVerifier.class);
    
    /**
     * Verifies that data was successfully written to the Iceberg table.
     * Checks table existence, snapshots, and validates data metrics.
     *
     * @param catalog the Hadoop catalog instance
     * @param tableId the table identifier to verify
     * @throws AssertionError if verification fails
     */
    public static void verifyDataWritten(HadoopCatalog catalog, TableIdentifier tableId) {
        assert catalog.tableExists(tableId) : "Iceberg table should exist";
        logger.info("Iceberg table exists: {}", tableId);
        
        Table table = catalog.loadTable(tableId);
        List<Snapshot> snapshots = new ArrayList<>();
        table.snapshots().forEach(snapshots::add);
        
        logger.info("Table has {} snapshots", snapshots.size());
        assert !snapshots.isEmpty() : "Table should have at least one snapshot";
        
        Snapshot latestSnapshot = snapshots.get(snapshots.size() - 1);
        Map<String, String> summary = latestSnapshot.summary();
        logger.info("Latest snapshot ID: {}, summary: {}", latestSnapshot.snapshotId(), summary);
        
        assert !summary.isEmpty() : "Snapshot should have summary information";
        
        String addedDataFiles = summary.get("added-data-files");
        String addedRecords = summary.get("added-records");
        String operation = summary.get("operation");
        
        logger.info("Snapshot operation: {}, added-data-files: {}, added-records: {}", 
                   operation, addedDataFiles, addedRecords);
        
        assert addedDataFiles != null && Integer.parseInt(addedDataFiles) > 0 : 
               "Snapshot should contain data files";
        assert addedRecords != null && Integer.parseInt(addedRecords) > 0 : 
               "Snapshot should contain records";
        assert "append".equals(operation) || operation == null : 
               "Snapshot should be an append operation, got: " + operation;
        
        logger.info("Successfully wrote data to Iceberg table: {} snapshots, {} data files, {} records", 
                   snapshots.size(), addedDataFiles, addedRecords);
        
        logDataSummary(table, addedRecords, addedDataFiles, summary, latestSnapshot);
    }
    
    /**
     * Logs a detailed summary of the data verification results.
     *
     * @param table the Iceberg table
     * @param addedRecords the number of records added
     * @param addedDataFiles the number of data files added
     * @param summary the snapshot summary
     * @param latestSnapshot the latest snapshot
     */
    private static void logDataSummary(Table table, String addedRecords, String addedDataFiles, 
                                     Map<String, String> summary, Snapshot latestSnapshot) {
        logger.info("=== ICEBERG DATA VERIFICATION ===");
        logger.info("✅ Data successfully written to Iceberg table!");
        logger.info("📊 Table location: {}", table.location());
        logger.info("📈 Total records: {}", addedRecords);
        logger.info("📁 Data files: {}", addedDataFiles);
        logger.info("💾 File size: {} bytes", summary.get("added-files-size"));
        logger.info("🎯 Snapshot ID: {}", latestSnapshot.snapshotId());
    }
}
