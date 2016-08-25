/*
 * ddlUtils is a subproject of the Apache DB project, and is licensed under
 * the Apache License, Version 2.0 (the "License"); you may not use these files
 * except in compliance with the License. You may obtain a copy of the License at:
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed
 * under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 *
 * Splice Machine, Inc. has modified this file.
 *
 * All Splice Machine modifications are Copyright 2012 - 2016 Splice Machine, Inc.,
 * and are licensed to you under the License; you may not use this file except in
 * compliance with the License.
 *
 * Unless required by applicable law or agreed to in writing, software distributed
 * under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 */

package org.apache.ddlutils.testutils;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URL;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.ddlutils.Platform;
import org.apache.ddlutils.model.Database;
import org.apache.ddlutils.model.Schema;
import org.apache.ddlutils.model.Table;
import org.apache.ddlutils.model.TableType;

/**
 * Utility methods for testing ddlutils.
 */
public class TestUtils {

    public static final String DRIVERCLASSNAME = "com.splicemachine.db.jdbc.ClientDriver";
    public static final String URL = "jdbc:splice://localhost:1527/splicedb";
    public static final String USERNAME = "splice";
    public static final String PASSWORD = "admin";

    /**
     * Set up a test by creating the schema and importing data in a transaction.<br/>
     * This is done be setting autocommit off on the connection and only committing if no error was encountered.
     * The original autocommit setting is preserved on the borrowed connection.
     * @param createScript the schema ddl with import procedures
     * @param platform the ddlutils platform specific to this database
     * @param connection the borrowed connection to use
     * @throws Exception throw and abort the transaction if any error
     */
    public static void setup(File createScript, Platform platform, Connection connection) throws Exception {
        boolean autoCommit = connection.getAutoCommit();
        try {
            connection.setAutoCommit(false);
            evaluateScript(createScript, platform, connection, false, true);
            connection.commit();
        } finally {
            connection.setAutoCommit(autoCommit);
        }
    }

    /**
     * Return the count of the given qualified table.
     * @param qualifiedTableName schemaName.tableName
     * @param connection connection to use.  Borrowed, unclosed.
     * @return table count
     * @throws SQLException
     */
    public static long countRecords(String qualifiedTableName, Connection connection) throws SQLException {
        long count;
        try (Statement s = connection.createStatement()) {
            ResultSet rs = s.executeQuery(String.format("SELECT COUNT(*) FROM %s", qualifiedTableName));
            assertTrue("No count!", rs.next());
            count = rs.getLong(1);
        }
        return count;
    }

    public static long countFileLines(File file) throws IOException {
        URL loc = file.toURI().toURL();
        try (BufferedReader reader = new BufferedReader(new InputStreamReader(loc.openStream()))) {
            return reader.lines().count();
        }
    }

    /**
     * Assert that the given schema name does not exist in teh database.
     * @param schemaName complete schema name or partial (eg: "MY_SCHEM&'0.
     * @param compareOperator operator string used in criteria. Either "=", for exact match, or "LIKE", for partial.
     * @param connection the connection to use.
     * @throws SQLException
     */
    public static void assertSchemaDropped(String schemaName, String compareOperator, Connection connection) throws SQLException {
        String query = String.format("select SCHEMANAME from SYS.SYSSCHEMAS WHERE SCHEMANAME %s '%s'", compareOperator, schemaName);
        try (Statement st = connection.createStatement()) {
            try (ResultSet rs = st.executeQuery(query)) {
                if (rs.next()) {
                    fail("Expected not to find schema '"+schemaName+"'.  Not dropped.");
                }
            }
        }
    }

    /**
     * Assert the count of the regex <code>pattern</code> in <code>searchString</code> match the <code>expected</code>.
     * @param pattern literal string or regex pattern.
     * @param searchString the string to search.
     * @param expected number of expected matches.
     */
    public static void assertCountString(String pattern, String searchString, int expected) {
        Pattern p = Pattern.compile(pattern);
        Matcher m = p.matcher(searchString);
        int count = 0;
        while (m.find()){
            count +=1;
        }
        assertEquals("String count wrong for '"+pattern+"' in:\n"+searchString, expected, count);
    }

    /**
     * Count the all tables in the given schema of the given type and assert it's the same as expected.
     * @param schema schema containing tables.
     * @param expected expected number of tables.
     * @param type {@link TableType type} of the table.
     */
    public static void assertCountTableType(Schema schema, int expected, TableType type) {
        assertNotNull("Schema is null.", schema);
        int actual = 0;
        for (Table table : schema.getTables()) {
            if (table.getType().equals(type)) ++actual;
        }
        assertEquals("Wrong count for tables of type '"+type+"' in schema '"+schema.getSchemaName()+"'", expected, actual);
    }

    /**
     * Count the tables in the given model of the given type and assert it's the same as expected.
     * @param model database model containing schemas which contain tables.
     * @param expected expected number of tables.
     * @param type {@link TableType type} of the table.
     */
    public static void assertCountTableType(Database model, int expected, TableType type) {
        assertNotNull("Database model is null.", model);
        int actual = 0;
        for (Schema schema : model.getSchemas()) {
            for (Table table : schema.getTables()) {
                if (table.getType().equals(type)) ++actual;
            }
        }
        assertEquals("Wrong count for tables of type '"+type+"' in model '"+model.getName()+"'", expected, actual);
    }

    /**
     * Compare the contents of two files ignoring whitespace.
     * <p/>
     * Comparing strings is a brittle way to test. A schema file must be generated first and validated with
     * eye balls and saved as expected content.  Then this file must be imported, its schema exported and the
     * SQL it generated compared with the expected.<br/>
     * Myriad issues can arise here.  Change internal data structure and model sorts differently, breaks.
     * Change the JDK the test was run under and model sorts differently, breaks.
     * <p/>
     * <b>Use with caution.</b>
     * @param expectedFile the file with the expected content.
     * @param actualFile the file with the actual content.
     * @throws IOException
     */
    public static void assertFilesCompare(File expectedFile, File actualFile) throws IOException {
        assertTrue("Expected File does not exist: '"+expectedFile+"'", expectedFile.exists());
        assertTrue("Actual File does not exist: '"+actualFile+"'", actualFile.exists());

        // remove all whitespace because line separators are throwing off comparison
        String expectedString = readFile(expectedFile.toPath()).replaceAll("\\s+","");

        // remove all whitespace because line separators are throwing off comparison
        String actualString = readFile(actualFile.toPath()).replaceAll("\\s+","");

        assertEquals("Files did not compare.", expectedString, actualString);
    }

    /**
     * Create the entities, schema and tables, using the contents of the SQL script.
     *
     * @param sqlScript the SQL script file to evaluate.
     * @param platform the database specific platform.
     * @param connection connection to use.
     * @param continueOnError whether to quit when encountering errors.
     * @throws Exception
     */
    public static void evaluateScript(File sqlScript, Platform platform, Connection connection,
                                      boolean continueOnError) throws Exception {
        evaluateScript(sqlScript, platform, connection, continueOnError, false);
    }

    /**
     * Create the entities, schema and tables, using the contents of the SQL script.
     *
     * @param sqlScript the SQL script file to evaluate.
     * @param platform the database specific platform.
     * @param connection connection to use.
     * @param continueOnError whether to quit when encountering errors.
     * @param filterPaths whether to replace path elements in scripts -- <code>&lt;USER.DIR&gt;</code> with
     *                    <code>System.getProperty("user.dir")</code>. Used to make integration tests user
     *                    independent.
     * @throws Exception
     */
    public static void evaluateScript(File sqlScript, Platform platform, Connection connection,
                                      boolean continueOnError, boolean filterPaths) throws Exception {
        assertTrue("File does not exist: '"+sqlScript+"'", sqlScript.exists());
        String schemaAsString = readFile(sqlScript.toPath());
        if (filterPaths) {
            schemaAsString = schemaAsString.replace("<USER.DIR>", System.getProperty("user.dir"));
        }
        platform.evaluateBatch(connection, platform.filterComments(schemaAsString), continueOnError);
    }

    /**
     * Find or create the file by the given base name in teh project target directory.
     * @param fileName base name of file.
     * @return the file under the project target directory.
     */
    public static File getOutputFile(String fileName) {
        return new File(System.getProperty("user.dir") + "/target/" + fileName);
    }

    /**
     * Look up <code>fileName</code> as a resource, create a file and return it.
     * @param fileName base name of the resource.
     * @return existing file.
     * @throws IOException if the resource fails to be found.
     */
    public static File getInputFileAsResource(String fileName, Class clazz) throws IOException {
        ClassLoader classLoader = clazz.getClassLoader();
        URL fileURL = classLoader.getResource(fileName);
        if (fileURL == null) throw new IOException("Unable to find resource '"+fileName+"'");
        return new File(fileURL.getFile());
    }

    /**
     * Look up <code>fileName</code> from a path under the CWD, create a file and return it.
     * @param filePath the path to the file under the project directory.
     * @return existing file.
     * @throws IOException if the resource fails to be found.
     */
    public static File getInputFile(String filePath) throws IOException {
        File file = new File(System.getProperty("user.dir") + filePath);
        if (! file.exists() || ! file.canRead()) throw new IOException("Unable to find file '"+file+"'");
        return file;
    }

    /**
     * Stringify the content of the test file represented by <code>path</code>.
     * @param path test file path
     * @return string content of teh file, unchanged.
     * @throws IOException
     */
    public static String readFile(Path path) throws IOException {
        byte[] encoded = Files.readAllBytes(path);
        return new String(encoded, Charset.defaultCharset());
    }
}
