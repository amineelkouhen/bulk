package org.cockroachlabs.bulk.crdb.monothreaded;

import com.opencsv.CSVReader;
import com.opencsv.exceptions.CsvValidationException;
import org.apache.logging.log4j.Logger;
import org.cockroachlabs.bulk.utils.Config;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.sql.*;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class BulkDeleteCRDB {

    public static void main(String[] args) throws IOException {

        Config conf = Config.getInstance();
        Logger logger = conf.getLogger(BulkDeleteCRDB.class);
        int size = conf.getBatchSize();

        try (Connection connection = conf.getCRDBConnection("crdb", false, false)) {
            // call the method to perform the load
            logger.info("Bulk Delete of " + size + " records in: " + deleteData(connection, prepareRecords(), size) + "ms");
        } catch (SQLException ex) {
            logger.error(ex);
        }
    }

    private static List<List<String>> prepareRecords() {
        List<List<String>> records = new ArrayList<>();
        try (CSVReader csvReader = new CSVReader(new InputStreamReader(Thread.currentThread().getContextClassLoader().getResourceAsStream("100000_books.csv")))) {
            String[] values = null;
            csvReader.skip(1);
            while ((values = csvReader.readNext()) != null) {
                records.add(Arrays.asList(values));
            }
        } catch (FileNotFoundException e) {
            throw new RuntimeException(e);
        } catch (IOException e) {
            throw new RuntimeException(e);
        } catch (CsvValidationException e) {
            throw new RuntimeException(e);
        }
        return records;
    }

    private static long deleteData(Connection connection, List<List<String>> records, int size) throws SQLException {
        int n = 0;
        connection.setAutoCommit(false); // Setting auto-commit off
        Statement stmt = connection.createStatement();
        long start = System.currentTimeMillis();
        while (n < size) {
            String deleteQuery = "DELETE FROM bookstore.books WHERE book_id = '" + records.get(n).get(1) + "'";
            stmt.addBatch(deleteQuery);
            n++;
        }
        stmt.executeBatch();
        connection.commit(); // commit

        long end = System.currentTimeMillis();
        return (end-start);
    }
}