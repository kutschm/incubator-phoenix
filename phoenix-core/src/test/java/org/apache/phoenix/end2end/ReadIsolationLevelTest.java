/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.phoenix.end2end;

import static org.apache.phoenix.util.TestUtil.*;
import static org.junit.Assert.*;

import java.sql.*;
import java.util.Properties;

import org.junit.Test;

import org.apache.phoenix.util.PhoenixRuntime;


public class ReadIsolationLevelTest extends BaseClientManagedTimeTest {
    private static final String ENTITY_ID1= "000000000000001";
    private static final String ENTITY_ID2= "000000000000002";
    private static final String VALUE1 = "a";
    private static final String VALUE2= "b";

    protected static void initTableValues(long ts, byte[][] splits) throws Exception {
        String tenantId = getOrganizationId();
        ensureTableCreated(getUrl(),ATABLE_NAME,splits, ts-2);

        Properties props = new Properties();
        props.setProperty(PhoenixRuntime.CURRENT_SCN_ATTRIB, Long.toString(ts));
        Connection upsertConn = DriverManager.getConnection(PHOENIX_JDBC_URL, props);
        // Insert all rows at ts
        PreparedStatement stmt = upsertConn.prepareStatement(
                "upsert into ATABLE VALUES (?, ?, ?)");
        stmt.setString(1, tenantId);
        stmt.setString(2, ENTITY_ID1);
        stmt.setString(3, VALUE1);
        stmt.execute(); // should commit too
        
        stmt.setString(2, ENTITY_ID2);
        stmt.setString(3, VALUE2);
        stmt.execute(); // should commit too

        upsertConn.commit();
        upsertConn.close();
    }

    @Test
    public void testStatementReadIsolationLevel() throws Exception {
        long ts = nextTimestamp();
        initTableValues(ts, null);
        String query = "SELECT A_STRING FROM ATABLE WHERE ORGANIZATION_ID=? AND ENTITY_ID=?";
        Properties props = new Properties();
        props.setProperty(PhoenixRuntime.CURRENT_SCN_ATTRIB, Long.toString(ts+1));
        Connection conn = DriverManager.getConnection(PHOENIX_JDBC_URL, props);
        conn.setAutoCommit(true);
        props.setProperty(PhoenixRuntime.CURRENT_SCN_ATTRIB, Long.toString(ts+2));
        Connection conn2 = DriverManager.getConnection(PHOENIX_JDBC_URL, props);
        props.setProperty(PhoenixRuntime.CURRENT_SCN_ATTRIB, Long.toString(ts+1));
        Connection conn3 = DriverManager.getConnection(PHOENIX_JDBC_URL, props);
        try {
            String tenantId = getOrganizationId();
            PreparedStatement statement = conn.prepareStatement(query);
            statement.setString(1, tenantId);
            statement.setString(2, ENTITY_ID1);
            ResultSet rs = statement.executeQuery();
            assertTrue(rs.next());
            assertEquals(VALUE1, rs.getString(1));
            assertFalse(rs.next());

            // Locate existing row and reset one of it's KVs.
            // Insert all rows at ts
            PreparedStatement stmt = conn.prepareStatement("upsert into ATABLE VALUES (?, ?, ?)");
            stmt.setString(1, tenantId);
            stmt.setString(2, ENTITY_ID1);
            stmt.setString(3, VALUE2);
            stmt.execute();
            
            PreparedStatement statement2 = conn2.prepareStatement(query);
            statement2.setString(1, tenantId);
            statement2.setString(2, ENTITY_ID1);
            // Run another query through same connection and make sure
            // you can find the new row
            rs = statement2.executeQuery();
            assertTrue(rs.next());
            assertEquals(VALUE2, rs.getString(1));
            assertFalse(rs.next());

            PreparedStatement statement3 = conn3.prepareStatement(query);
            statement3.setString(1, tenantId);
            statement3.setString(2, ENTITY_ID1);
            rs = statement3.executeQuery();
            assertTrue(rs.next());
            assertEquals(VALUE1, rs.getString(1));
            assertFalse(rs.next());
        } finally {
            conn.close();
            conn2.close();
        }
    }

    @Test
    public void testConnectionReadIsolationLevel() throws Exception {
        long ts = nextTimestamp();
        initTableValues(ts, null);
        String query = "SELECT A_STRING FROM ATABLE WHERE ORGANIZATION_ID=? AND ENTITY_ID=?";
        String url = PHOENIX_JDBC_URL + ";" + PhoenixRuntime.CURRENT_SCN_ATTRIB + "=" + (ts+1);
        Connection conn = DriverManager.getConnection(url, TEST_PROPERTIES);
        conn.setAutoCommit(true);
        try {
            String tenantId = getOrganizationId();
            PreparedStatement statement = conn.prepareStatement(query);
            statement.setString(1, tenantId);
            statement.setString(2, ENTITY_ID1);
            ResultSet rs = statement.executeQuery();
            assertTrue(rs.next());
            assertEquals(VALUE1, rs.getString(1));
            assertFalse(rs.next());

            // Locate existing row and reset one of it's KVs.
            // Insert all rows at ts
            PreparedStatement stmt = conn.prepareStatement("upsert into ATABLE VALUES (?, ?, ?)");
            stmt.setString(1, tenantId);
            stmt.setString(2, ENTITY_ID1);
            stmt.setString(3, VALUE2);
            stmt.execute();
            
            // Run another query through same connection and make sure
            // you can't find the new row
            rs = statement.executeQuery();
            assertTrue(rs.next());
            assertEquals(VALUE1, rs.getString(1));
            assertFalse(rs.next());
        } finally {
            conn.close();
        }
    }
}
