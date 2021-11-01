package org.dqc.check.util;

import org.apache.log4j.Logger;

import java.io.InputStream;
import java.sql.*;
import java.util.Properties;

public class Utils {

    private static final Logger logger = Logger.getLogger(Utils.class);
    static InputStream in;
    static Properties properties = null;

    private static Utils utils;

    private Utils(Properties properties) {
        this.properties = properties;
    }

    public static Utils getInstance(Properties properties) {
        if (utils == null)
            utils = new Utils(properties);
        return utils;
    }

    /**
     * Get a new MySQL connection
     * **/
    public Properties getProperties() throws ClassNotFoundException,
            SQLException {
        return this.properties;
    }

    /**
     * Get a new MySQL connection
     * **/
    public Connection getMySQLConfConnection() throws ClassNotFoundException,
            SQLException {
        Class.forName(properties.getProperty("mySQLConfConnectionDriver"));
        Connection conn = DriverManager.getConnection(
                properties.getProperty("mySQLConfConnectionURL"),
                properties.getProperty("mySQLConfConnectionUser"),
                properties.getProperty("mySQLConfConnectionPassword"));
        return conn;
    }

    /**
     * Get a new MySQL connection
     * **/
    public Connection getMySQLDataConnection() throws ClassNotFoundException,
            SQLException {
        Class.forName(properties.getProperty("mySQLDataConnectionDriver"));
        Connection conn = DriverManager.getConnection(
                properties.getProperty("mySQLDataConnectionURL"),
                properties.getProperty("mySQLDataConnectionUser"),
                properties.getProperty("mySQLDataConnectionPassword"));
        return conn;
    }

    /**
     * Get a new Hive connection
     * **/
    public Connection getHiveTkioConnection() throws ClassNotFoundException,
            SQLException {

        Class.forName(properties.getProperty("hiveTkioConnectionDriver"));
        Connection conn = DriverManager.getConnection(
                properties.getProperty("hiveTkioConnectionURL"),
                properties.getProperty("hiveTkioConnectionUser"),
                properties.getProperty("hiveTkioConnectionPassword"));
        return conn;
    }

    public void release(ResultSet rs, PreparedStatement pstmt, Connection conn) {
        try {
            if (rs != null) {
                rs.close();
                rs = null;
            }
        } catch (SQLException e) {
            logger.error(e.getMessage(), e);
        } finally {
            try {
                if (pstmt != null) {
                    pstmt.close();
                    pstmt = null;
                }
            } catch (SQLException e) {
                logger.error(e.getMessage(), e);
            } finally {
                try {
                    if (conn != null) {
                        conn.close();
                        conn = null;
                    }
                } catch (SQLException e) {
                    logger.error(e.getMessage(), e);
                }
            }
        }
    }
}

