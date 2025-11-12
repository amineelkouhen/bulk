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

public class BulkInsertCRDB {

    public static void main(String[] args) throws IOException {

        Config conf = Config.getInstance();
        Logger logger = conf.getLogger(BulkInsertCRDB.class);
        int size = conf.getBatchSize();

        try (Connection connection = conf.getCRDBConnection("crdb", true, false)) {
            prepareTable(connection);

            // call the method to perform the load
            logger.info("Bulk Insert of " + size + " records in: " + insertData(connection, prepareRecords(), size) + "ms");
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

    private static void prepareTable(Connection connection) throws SQLException {
        Statement stmt = connection.createStatement();
        String dropTableQuery = "DROP TABLE IF EXISTS bookstore.books";
        stmt.execute(dropTableQuery);

        StringBuilder createTableQueryBuilder = new StringBuilder("CREATE TABLE IF NOT EXISTS bookstore.books ")
                .append("(book_id UUID PRIMARY KEY, ")
                .append("title STRING NOT NULL, ")
                .append("author STRING NOT NULL, ")
                .append("price FLOAT NOT NULL, ")
                .append("format STRING NOT NULL, ")
                .append("publish_date DATE NOT NULL)");
        String createTableQuery = createTableQueryBuilder.toString();
        stmt = connection.createStatement();
        stmt.execute(createTableQuery);
    }

    private static long insertData(Connection connection, List<List<String>> records, int size) throws SQLException {
        int n = 0;
        connection.setAutoCommit(false); // Setting auto-commit off
        Statement stmt = connection.createStatement();
        long start = System.currentTimeMillis();
        while (n < size) {
            String insertQuery = "INSERT INTO bookstore.books (book_id, title, author, price, format, publish_date) VALUES ('" + records.get(n).get(1) + "' ,'" + records.get(n).get(2) + "', '" + records.get(n).get(3) + "', " + records.get(n).get(4) + ", '" + records.get(n).get(5) + "', '" + records.get(n).get(6) + "')";
            stmt.addBatch(insertQuery);
            n++;
        }
        stmt.executeBatch(); // execute in parallel
        connection.commit(); // commit

        long end = System.currentTimeMillis();
       return (end-start);
    }
}