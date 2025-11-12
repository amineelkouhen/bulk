package org.cockroachlabs.bulk.oracle.partitioned;

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
import java.util.Collections;
import java.util.List;
import java.util.concurrent.*;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class BulkMultithreadedInsertPartitionedORCL {
    public static void main(String[] args) throws IOException, SQLException {

        Config conf = Config.getInstance();
        Logger logger = conf.getLogger(BulkMultithreadedInsertPartitionedORCL.class);
        int batchSize = conf.getBatchSize();

        List<List<String>> records = prepareRecords();
        int partitions = conf.getPartitions("oracle");
        int splits = batchSize / partitions;

        try (Connection connection = conf.getConnection("oracle")) {
            prepareTable(connection, batchSize, partitions);

            List<Callable<Long>> tasks = new ArrayList<Callable<Long>>();
            for (int i = 0; i < partitions; i++) {
                int idx = i;
                Callable<Long> c = new Callable<Long>() {
                    @Override
                    public Long call() throws Exception {
                        return insertData(connection, records.subList((idx * splits), (splits * (idx + 1))), splits);
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
                logger.info(String.format("Parallel Bulk Insert of %d records took: %d ms", batchSize, elapsed));
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
        String dropTableQuery = "   BEGIN \n" +
                "       EXECUTE IMMEDIATE 'DROP TABLE bookstore.books'; \n" +
                "   EXCEPTION \n" +
                "       WHEN OTHERS THEN \n" +
                "           IF SQLCODE != -942 THEN \n" +
                "               RAISE; \n" +
                "           END IF; \n" +
                "   END; \n";
        stmt.execute(dropTableQuery);

        int splits = batchSize/partitions;
        StringBuilder createTableQueryBuilder = new StringBuilder("CREATE TABLE bookstore.books")
                .append("(rw_id NUMBER(10) PRIMARY KEY, ")
                .append("book_id VARCHAR2(36), ")
                .append("title VARCHAR2(255) NOT NULL, ")
                .append("author VARCHAR2(255) NOT NULL, ")
                .append("price NUMBER(7,2) NOT NULL, ")
                .append("format VARCHAR2(255) NOT NULL, ")
                .append("publish_date DATE NOT NULL) ")
                .append("PARTITION BY RANGE(rw_id) ")
                .append("(");

        for (int i = 1; i <= partitions; i++){
            createTableQueryBuilder.append("PARTITION books_part_" + i + " VALUES LESS THAN(" + ((i * splits) + 1) + ")");
            if(i < partitions) createTableQueryBuilder.append(", ");
        }
        createTableQueryBuilder.append(")");

        String createTableQuery = createTableQueryBuilder.toString();
        stmt.execute(createTableQuery);
    }

    private static long insertData(Connection connection, List<List<String>> records, int size) throws SQLException {
        int n = 0;
        connection.setAutoCommit(false); // Setting auto-commit off
        Statement stmt = connection.createStatement();
        long start = System.currentTimeMillis();
        while (n < size) {
            String insertQuery = "INSERT INTO bookstore.books (rw_id, book_id, title, author, price, format, publish_date) VALUES (" + records.get(n).get(0) + ", '" + records.get(n).get(1) + "' ,'" + records.get(n).get(2) + "', '" + records.get(n).get(3) + "', " + records.get(n).get(4) + ", '" + records.get(n).get(5) + "', TO_DATE('" + records.get(n).get(6) + "', 'yyyy/mm/dd'))";
            stmt.addBatch(insertQuery);
            n++;
        }
        stmt.executeLargeBatch();
        connection.commit(); // commit

        long end = System.currentTimeMillis();
        return (end-start);
    }
}