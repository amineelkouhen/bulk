package org.cockroachlabs.bulk.db2.partitioned;

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
import java.util.concurrent.*;

public class BulkMixedCRUDMultithreadedPartitionedDB2 {

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
        Logger logger = conf.getLogger(BulkMixedCRUDMultithreadedPartitionedDB2.class);
        int batchSize = conf.getBatchSize();
        int partitions = conf.getPartitions("db2");
        int splits = batchSize / partitions;

        try (Connection connection = conf.getConnection("db2")) {
            prepareTable(connection, batchSize, partitions);

            List<Callable<Long>> tasks = new ArrayList<Callable<Long>>();
            for (int i = 0; i < partitions; i++) {
                int idx = i;
                Callable<Long> c = new Callable<Long>() {
                    @Override
                    public Long call() throws Exception {
                        return processCRUDData(connection, prepareRecords().subList((idx * splits), (splits * (idx + 1))), splits);
                    }
                };
                tasks.add(c);
            }
            ExecutorService exec = Executors.newFixedThreadPool(partitions);
            try {
                long start = System.currentTimeMillis();
                List<Future<Long>> results = exec.invokeAll(tasks);
                int sum = 0;
                int j = 0;
                for (Future<Long> fr : results) {
                    ++j;
                    sum += fr.get();
                    System.out.println(String.format("Thread %d waited %d ms", j, fr.get()));
                }
                long elapsed = System.currentTimeMillis() - start;
                logger.info(String.format("Parallel Bulk CRUD of %d records took: %d ms", batchSize, elapsed));
            } catch (ExecutionException e) {
                throw new RuntimeException(e);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            } finally {
                exec.shutdown();
            }
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

    private static void prepareTable(Connection connection, int batchSize, int partitions) throws SQLException {
        Statement stmt = connection.createStatement();
        String dropTableQuery = "DROP TABLE IF EXISTS books";
        stmt.execute(dropTableQuery);

        int splits = batchSize/partitions;

        StringBuilder createTableQueryBuilder = new StringBuilder("CREATE TABLE IF NOT EXISTS books ")
                .append("(row_id int PRIMARY KEY NOT NULL, ")
                .append("book_id VARCHAR(36) NOT NULL, ")
                .append("title VARCHAR(255) NOT NULL, ")
                .append("author VARCHAR(255) NOT NULL, ")
                .append("price FLOAT NOT NULL, ")
                .append("format VARCHAR(255) NOT NULL, ")
                .append("publish_date DATE NOT NULL)")
                .append("PARTITION BY RANGE (row_id) (STARTING FROM (1)")
                .append("ENDING AT (" + batchSize + ") EVERY (" + splits + "))");

        String createTableQuery = createTableQueryBuilder.toString();
        stmt = connection.createStatement();
        stmt.execute(createTableQuery);
    }

    private static long processCRUDData(Connection connection, List<List<String>> records, int size) throws SQLException {
        int n = 0;
        //connection.setAutoCommit(false); // Setting auto-commit off
        Statement stmt = connection.createStatement();
        long start = System.currentTimeMillis();
        while (n < size) {
            Operation crudOperation = Operation.randomCRUD();
            switch(crudOperation) {
                case CREATE:
                    String insertQuery = "INSERT INTO books (row_id, book_id, title, author, price, format, publish_date) VALUES (" + records.get(n).get(0) + ", '" + records.get(n).get(1) + "' ,'" + records.get(n).get(2) + "', '" + records.get(n).get(3) + "', " + records.get(n).get(4) + ", '" + records.get(n).get(5) + "', '" + records.get(n).get(6) + "')";
                    stmt.addBatch(insertQuery);
                    break;
                case READ:
                    String selectQuery = "SELECT * FROM books WHERE row_id = " + records.get(n).get(0) ;
                    //stmt.addBatch(selectQuery);
                    break;
                case UPDATE:
                    String updateQuery = "UPDATE books SET price = (price * 0.8) WHERE row_id = " + records.get(n).get(0) ;
                    stmt.addBatch(updateQuery);
                    break;
                case DELETE:
                    String deleteQuery = "DELETE FROM books WHERE row_id = " + records.get(n).get(0) ;
                    stmt.addBatch(deleteQuery);
                    break;
            }
            n++;
        }
        stmt.executeBatch(); // execute in parallel
        //connection.commit(); // commit

        long end = System.currentTimeMillis();
        return (end-start);
    }
}