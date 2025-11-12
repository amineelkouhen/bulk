package org.cockroachlabs.bulk.mssql;

import com.opencsv.CSVReader;
import com.opencsv.exceptions.CsvValidationException;
import org.apache.logging.log4j.Logger;
import org.cockroachlabs.bulk.utils.Config;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Random;

public class BulkMixedCRUDMSSQL {

    public enum Operation {
        CREATE, READ, UPDATE, DELETE;

        private static final Random PRNG = new Random();

        public static Operation randomCRUD()  {
            Operation[] operations = values();
            return operations[PRNG.nextInt(operations.length)];
        }
    }

    public static void main(String[] args) throws IOException {

        Config conf = Config.getInstance();
        Logger logger = conf.getLogger(BulkMixedCRUDMSSQL.class);
        int size = conf.getBatchSize();

        try (Connection connection = conf.getConnection("mssql")) {
            prepareTable(connection);

            // call the method to perform the load
            logger.info("Bulk Insert of " + size + " records in: " + processCRUDData(connection, prepareRecords(), size) + "ms");
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
        String dropTableQuery = "IF OBJECT_ID('bookstore.dbo.books', 'U') IS NOT NULL \n" + "  DROP TABLE bookstore.dbo.books";
        stmt.execute(dropTableQuery);

        StringBuilder createTableQueryBuilder = new StringBuilder("CREATE TABLE bookstore.dbo.books")
                .append("(book_id UNIQUEIDENTIFIER PRIMARY KEY, ")
                .append("title VARCHAR(255) NOT NULL, ")
                .append("author VARCHAR(255) NOT NULL, ")
                .append("price FLOAT NOT NULL, ")
                .append("format VARCHAR(255) NOT NULL, ")
                .append("publish_date DATE NOT NULL)");
        String createTableQuery = createTableQueryBuilder.toString();
        stmt = connection.createStatement();
        stmt.execute(createTableQuery);
    }

    private static long processCRUDData(Connection connection, List<List<String>> records, int size) throws SQLException {
        int n = 0;
        connection.setAutoCommit(false); // Setting auto-commit off
        Statement stmt = connection.createStatement();
        long start = System.currentTimeMillis();
        while (n < size) {
            Operation crudOperation = Operation.randomCRUD();
            switch(crudOperation) {
                case CREATE:
                    String insertQuery = "INSERT INTO bookstore.dbo.books (book_id, title, author, price, format, publish_date) VALUES ('" + records.get(n).get(1) + "' ,'" + records.get(n).get(2) + "', '" + records.get(n).get(3) + "', " + records.get(n).get(4) + ", '" + records.get(n).get(5) + "', '" + records.get(n).get(6) + "')";
                    stmt.addBatch(insertQuery);
                    break;
                case READ:
                    String selectQuery = "SELECT * FROM bookstore.dbo.books WHERE book_id = '" + records.get(n).get(1) + "'";
                    stmt.addBatch(selectQuery);
                    break;
                case UPDATE:
                    String updateQuery = "UPDATE bookstore.dbo.books SET price = (price * 0.8) WHERE book_id = '" + records.get(n).get(1) + "'";
                    stmt.addBatch(updateQuery);
                    break;
                case DELETE:
                    String deleteQuery = "DELETE FROM bookstore.dbo.books WHERE book_id = '" + records.get(n).get(1) + "'";
                    stmt.addBatch(deleteQuery);
                    break;
            }
            n++;
        }
        stmt.executeBatch(); // execute in parallel
        connection.commit(); // commit

        long end = System.currentTimeMillis();
        return (end-start);
    }
}