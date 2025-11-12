package org.cockroachlabs.bulk.utils;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.core.LoggerContext;
import org.apache.logging.log4j.core.config.Configuration;
import org.apache.logging.log4j.core.config.LoggerConfig;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

public final class Config {

    private static Config INSTANCE;
    private static final Properties prop = new Properties();

    private Config() throws IOException{
        // Refetch LoggerConfig.
        LoggerContext ctx = (LoggerContext) LogManager.getContext(false);
        Configuration config = ctx.getConfiguration();
        LoggerConfig loggerConfig = config.getLoggerConfig(LogManager.ROOT_LOGGER_NAME);
        loggerConfig.setLevel(Level.ALL);
        ctx.updateLoggers();

        //load a properties file from class path, inside static method
        prop.load(Thread.currentThread().getContextClassLoader().getResourceAsStream("config.properties"));
    }

    public static Config getInstance() throws IOException{
        if(INSTANCE == null) {
            INSTANCE = new Config();
        }
        return INSTANCE;
    }

    public static Properties getProperties(){
        return prop;
    }

    public static Logger getLogger(Class<?> clazz){
        return LogManager.getLogger(clazz);
    }

    public static List<Connection> getCRDBConnections(String prefix, boolean isInsertOptimized, boolean isUpdateOptimized) throws SQLException {
        String[] hosts = prop.getProperty(prefix + ".hosts").split(",");
        String schema = prop.getProperty(prefix + ".schema");
        String user = prop.getProperty(prefix + ".user");
        String password = prop.getProperty(prefix + ".password");
        List<Connection> connections = new ArrayList<>();

        for (String host: hosts)
            connections.add(DriverManager.getConnection(getCRDBConnectionString("jdbc:postgresql://" + host + ":26257/" + schema, isInsertOptimized, isUpdateOptimized), user, password));

        return connections;
    }

    public static Connection getConnection(String prefix) throws SQLException {
        //get the property value and print it out
        String connectionString = prop.getProperty(prefix + ".connection.string");
        String user = prop.getProperty(prefix + ".user");
        String password = prop.getProperty(prefix + ".password");

        return DriverManager.getConnection(connectionString, user, password);
    }

    public int getBatchSize() {
        return Integer.parseInt(prop.getProperty("batch.size"));
    }

    public int getPartitions(String prefix) {
        return Integer.parseInt(prop.getProperty(prefix + ".partitions"));
    }

    public static Connection getCRDBConnection(String prefix, boolean isInsertOptimized, boolean isUpdateOptimized) throws SQLException {
        //get the property value and print it out
        String connectionString = prop.getProperty(prefix + ".connection.string");
        String user = prop.getProperty(prefix + ".user");
        String password = prop.getProperty(prefix + ".password");
        return DriverManager.getConnection(getCRDBConnectionString(connectionString, isInsertOptimized, isUpdateOptimized), user, password);
    }

    public static String getCRDBConnectionString(String connectionString, boolean isInsertOptimized, boolean isUpdateOptimized){
        if (isInsertOptimized)
            return connectionString + "?rewriteBatchedInserts=true";
        else if (isUpdateOptimized)
            return connectionString + "?rewriteBatchedUpdates=true";
        else
            return connectionString;
    }
}
