/*
 * Copyright 2012 - 2016 Splice Machine, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use
 * this file except in compliance with the License. You may obtain a copy of the
 * License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed
 * under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 */

package com.splicemachine.derby.impl.sql.execute.actions;

import java.sql.SQLException;

import org.junit.Assert;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.RuleChain;
import org.junit.rules.TestRule;

import com.splicemachine.derby.test.framework.SpliceSchemaWatcher;
import com.splicemachine.derby.test.framework.SpliceTableWatcher;
import com.splicemachine.derby.test.framework.SpliceWatcher;

/**
 * @author Scott Fines
 *         Created on: 8/4/13
 */
public class CompoundPrimaryKeyIT extends AbstractIndexTest {
    private static SpliceWatcher spliceClassWatcher = new SpliceWatcher();
    private static String CLASS_NAME = CompoundPrimaryKeyIT.class.getSimpleName().toUpperCase();
    private static SpliceSchemaWatcher spliceSchemaWatcher = new SpliceSchemaWatcher(CLASS_NAME);

    private static SpliceTableWatcher twoCtgColumns                = new SpliceTableWatcher("TWO_CONTIGUOUS",spliceSchemaWatcher.schemaName,"(a int, b float, c int, d double, PRIMARY KEY(a,b))");
    private static SpliceTableWatcher twoNonCtgColumns             = new SpliceTableWatcher("TWO_NONCONTIGUOUS",spliceSchemaWatcher.schemaName,"(a int, b float, c int, d double,PRIMARY KEY(a,c))");
    private static SpliceTableWatcher twoOutOfOrderNonCtgColumns   = new SpliceTableWatcher("TWO_NONCONTIGUOUS_OUT_OF_ORDER",spliceSchemaWatcher.schemaName,"(a int, b float, c int, d double, PRIMARY KEY(c,a))");
    private static SpliceTableWatcher threeCtgColumns              = new SpliceTableWatcher("THREE_CTG",spliceSchemaWatcher.schemaName,"(a int, b float, c int, d double, PRIMARY KEY(b,c,d))");
    private static SpliceTableWatcher threeNonCtgColumns           = new SpliceTableWatcher("THREE_NCTG",spliceSchemaWatcher.schemaName,"(a int, b float, c int, d double, PRIMARY KEY(a,c,d))");
    private static SpliceTableWatcher threeOutOfOrderNonCtgColumns = new SpliceTableWatcher("THREE_OO_NCTG",spliceSchemaWatcher.schemaName,"(a int, b float, c int, d double, PRIMARY KEY(c,a,d))");

    @ClassRule
    public static TestRule chain = RuleChain.outerRule(spliceClassWatcher)
            .around(twoCtgColumns)
            .around(twoNonCtgColumns)
            .around(twoOutOfOrderNonCtgColumns)
            .around(threeCtgColumns)
            .around(threeNonCtgColumns)
            .around(threeOutOfOrderNonCtgColumns);


    @Test
    public void testCanInsertIntoTwoIndexColumns() throws Exception {
        insertData(3,twoCtgColumns.toString());
        assertCorrectScan(3,twoCtgColumns.toString());

        //make sure no duplicates can be added
        try{
            insertData(1,twoCtgColumns.toString());
        }catch(SQLException sqle){
            Assert.assertEquals("Incorrect error message received!","23505",sqle.getSQLState());
        }
    }

    @Test
    public void testCanInsertIntoTwoNonCtgIndexColumns() throws Exception {
        insertData(3,twoNonCtgColumns.toString());
        assertCorrectScan(3,twoNonCtgColumns.toString());

        //make sure no duplicates can be added
        try{
            insertData(1,twoNonCtgColumns.toString());
        }catch(SQLException sqle){
            Assert.assertEquals("Incorrect error message received!","23505",sqle.getSQLState());
        }
    }

    @Test
    public void testCanInsertIntoTwoNonCtgIndexColumnsOutOfOrder() throws Exception {
        insertData(3,twoOutOfOrderNonCtgColumns.toString());
        assertCorrectScan(3,twoOutOfOrderNonCtgColumns.toString());

        //make sure no duplicates can be added
        try{
            insertData(1,twoOutOfOrderNonCtgColumns.toString());
        }catch(SQLException sqle){
            Assert.assertEquals("Incorrect error message received!","23505",sqle.getSQLState());
        }
    }

    @Test
    public void testCanInsertIntoThreeCtgIndexColumns() throws Exception {
        insertData(3,threeCtgColumns.toString());
        assertCorrectScan(3,threeCtgColumns.toString());

        //make sure no duplicates can be added
        try{
            insertData(1,threeCtgColumns.toString());
        }catch(SQLException sqle){
            Assert.assertEquals("Incorrect error message received!","23505",sqle.getSQLState());
        }
    }

    @Test
    public void testCanInsertIntoThreeNonCtgIndexColumns() throws Exception {
        insertData(3,threeNonCtgColumns.toString());
        assertCorrectScan(3,threeNonCtgColumns.toString());
        //make sure no duplicates can be added
        try{
            insertData(1,threeNonCtgColumns.toString());
        }catch(SQLException sqle){
            Assert.assertEquals("Incorrect error message received!","23505",sqle.getSQLState());
        }
    }

    @Test
    public void testCanInsertIntoThreeOutOfOrderNonCtgIndexColumns() throws Exception {
        insertData(3,threeOutOfOrderNonCtgColumns.toString());
        assertCorrectScan(3,threeOutOfOrderNonCtgColumns.toString());
        //make sure no duplicates can be added
        try{
            insertData(1,threeOutOfOrderNonCtgColumns.toString());
        }catch(SQLException sqle){
            Assert.assertEquals("Incorrect error message received!","23505",sqle.getSQLState());
        }
    }
}
