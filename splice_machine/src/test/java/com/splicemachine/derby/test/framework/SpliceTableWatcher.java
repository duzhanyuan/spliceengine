package com.splicemachine.derby.test.framework;

import java.sql.*;

import com.splicemachine.test_dao.TableDAO;
import org.apache.commons.dbutils.DbUtils;
import org.apache.log4j.Logger;
import org.junit.rules.TestWatcher;
import org.junit.runner.Description;

public class SpliceTableWatcher extends TestWatcher {
    private static final Logger LOG = Logger.getLogger(SpliceTableWatcher.class);
    public String tableName;
    protected String schemaName;
    protected String createString;
    protected String userName;
    protected String password;

    public SpliceTableWatcher(String tableName,String schemaName, String createString) {
        this.tableName = tableName.toUpperCase();
        this.schemaName = schemaName.toUpperCase();
        this.createString = createString;
    }

    public SpliceTableWatcher(String tableName, String schemaName, String createString, String userName, String password) {
        this(tableName,schemaName,createString);
        this.userName = userName;
        this.password = password;
    }

    @Override
    protected void starting(Description description) {
        start();
        super.starting(description);
    }

    @Override
    protected void finished(Description description) {
        trace("finished");
    }

    public void importData(String filename) {
        Connection connection = null;
        PreparedStatement ps = null;
        try {
            connection = SpliceNetConnection.getConnection();
            ps = connection.prepareStatement("call SYSCS_UTIL.IMPORT_DATA (?, ?, null,?,',',null,null,null,null,1,null,true,'utf-8')");
            ps.setString(1,schemaName);
            ps.setString(2,tableName);
            ps.setString(3,filename);
            ResultSet rs = ps.executeQuery();
            while (rs.next()) {

            }
        } catch (Exception e) {
            error(e);
            throw new RuntimeException(e);
        } finally {
            DbUtils.closeQuietly(ps);
            DbUtils.commitAndCloseQuietly(connection);
        }
    }

    public void importData(String filename, String timestamp) {
        Connection connection = null;
        PreparedStatement ps = null;
        try {
            connection = SpliceNetConnection.getConnection();
            ps = connection.prepareStatement("call SYSCS_UTIL.IMPORT_DATA (?, ?, null,?,',',null,?,null,null,0,null,true,null)");
            ps.setString(1,schemaName);
            ps.setString(2,tableName);
            ps.setString(3,filename);
            ps.setString(4, timestamp);
            ps.executeQuery();
        } catch (Exception e) {
            error(e);
            throw new RuntimeException(e);
        } finally {
            DbUtils.closeQuietly(ps);
            DbUtils.commitAndCloseQuietly(connection);
        }
    }

    @Override
    public String toString() {
        return schemaName+"."+tableName;
    }

    public String getSchema() {
        return schemaName;
    }

    //-----------------------------------------------------------------------------------------
    // The following methods are for tagging the log messages with additional information
    // related to the schema and table.
    //-----------------------------------------------------------------------------------------

    /**
     * Tag the message with extra information (schema and table names) if the message is a String.
     * @param message  message to be potentially tagged
     * @param schema  name of schema
     * @param table  name of table
     */
    protected static Object tag(Object message, String schema, String table) {
        if (message instanceof String) {
            return String.format("[%s.%s] %s", schema, table, message);
        } else {
            return message;
        }
    }

    /**
     * Tag the message with extra information if the message is a String.
     * @param message  message to be potentially tagged
     */
    protected Object tag(Object message) {
        return tag(message, schemaName, tableName);
    }

    protected void trace(Object message) {
        LOG.trace(tag(message));
    }

    protected void error(Object message) {
        LOG.error(tag(message));
    }

    protected void error(Object message, Throwable t) {
        LOG.error(tag(message), t);
    }

    public void start() {
        trace("Starting");
        Statement statement = null;
        ResultSet rs;
        Connection connection;
        synchronized(SpliceTableWatcher.class){
            try{
                connection=(userName==null)?SpliceNetConnection.getConnection():SpliceNetConnection.getConnectionAs(userName,password);
                rs=connection.getMetaData().getTables(null,schemaName,tableName,null);
            }catch(Exception e){
                error("Error when fetching metadata",e);
                throw new RuntimeException(e);
            }
        }
        TableDAO tableDAO = new TableDAO(connection);

        try {
            if (rs.next()) {
                tableDAO.drop(schemaName, tableName);
            }
            connection.commit();
        } catch (SQLException e) {
            error("Error when dropping tables",e);
        }

        try{
            statement = connection.createStatement();
            statement.execute(String.format("create table %s.%s %s",schemaName,tableName,createString));
            connection.commit();
        } catch (Exception e) {
            error("Create table statement is invalid. Statement = "+ String.format("create table %s.%s %s",schemaName,tableName,createString),e);
            throw new RuntimeException(e);
        } finally {
            DbUtils.closeQuietly(rs);
            DbUtils.closeQuietly(statement);
            DbUtils.commitAndCloseQuietly(connection);
        }
    }
}