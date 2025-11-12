package org.cockroachlabs.bulk.csql.partitioned;

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
import java.util.concurrent.*;

public class BulkMultithreadedDeletePartitionedCSQL {
    public static void main(String[] args) throws IOException {

        Config conf = Config.getInstance();
        Logger logger = conf.getLogger(BulkMultithreadedDeletePartitionedCSQL.class);
        int batchSize = conf.getBatchSize();
        int partitions = conf.getPartitions("csql");
        int splits = batchSize / partitions;

        List<List<String>> records = prepareRecords();
        try (Connection connection = conf.getConnection("csql")) {
            List<Callable<Long>> tasks = new ArrayList<Callable<Long>>();
            for (int i = 0; i < partitions; i++) {
                int idx = i;
                Callable<Long> c = new Callable<Long>() {
                    @Override
                    public Long call() throws Exception {
                        return deleteData(connection, records.subList((idx * splits), (splits * (idx + 1))), splits);
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
                logger.info(String.format("Parallel Bulk Delete of %d records took: %d ms", batchSize, elapsed));
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

    private static long deleteData(Connection connection, List<List<String>> records, int size) throws SQLException {
        int n = 0;
        connection.setAutoCommit(false); // Setting auto-commit off
        Statement stmt = connection.createStatement();
        long start = System.currentTimeMillis();
        while (n < size) {
            String deleteQuery = "DELETE FROM books WHERE row_id = " + records.get(n).get(0);
            stmt.addBatch(deleteQuery);
            n++;
        }
        stmt.executeBatch();
        connection.commit(); // commit

        long end = System.currentTimeMillis();
        return (end-start);
    }
}