package com.hazelcast.jet.sql.impl.connector.jdbc;

import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import java.util.Arrays;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.when;

public class H2UpsertQueryBuilderTest {
    @Mock
    JdbcTable jdbcTable;

    @Before
    public void setUp() {
        MockitoAnnotations.openMocks(this);
    }

    @Test
    public void testGetMergeClause() {
        when(jdbcTable.getExternalName()).thenReturn("table1");
        when(jdbcTable.dbFieldNames()).thenReturn(Arrays.asList("field1", "field2"));

        H2UpsertQueryBuilder builder = new H2UpsertQueryBuilder(jdbcTable);
        StringBuilder stringBuilder = new StringBuilder();
        builder.getMergeClause(jdbcTable, stringBuilder);

        String insertClause = stringBuilder.toString();
        assertEquals("MERGE INTO table1 (field1,field2) ", insertClause);
    }

    @Test
    public void testGetKeyClause() {

        when(jdbcTable.getExternalName()).thenReturn("table1");
        when(jdbcTable.getPrimaryKeyList()).thenReturn(Arrays.asList("pk1", "pk2"));
        when(jdbcTable.dbFieldNames()).thenReturn(Arrays.asList("field1", "field2"));

        H2UpsertQueryBuilder builder = new H2UpsertQueryBuilder(jdbcTable);
        StringBuilder stringBuilder = new StringBuilder();
        builder.getKeyClause(jdbcTable, stringBuilder);

        String keyClause = stringBuilder.toString();
        assertEquals("KEY (pk1,pk2 ) ", keyClause);

    }

    @Test
    public void testGetValuesClause() {

        when(jdbcTable.getExternalName()).thenReturn("table1");
        when(jdbcTable.dbFieldNames()).thenReturn(Arrays.asList("field1", "field2"));

        H2UpsertQueryBuilder builder = new H2UpsertQueryBuilder(jdbcTable);
        StringBuilder stringBuilder = new StringBuilder();
        builder.getValuesClause(jdbcTable, stringBuilder);

        String valueClause = stringBuilder.toString();
        assertEquals("VALUES (?, ?)", valueClause);
    }

    @Test
    public void testQuery() {

        when(jdbcTable.getExternalName()).thenReturn("table1");
        when(jdbcTable.getPrimaryKeyList()).thenReturn(Arrays.asList("pk1", "pk2"));
        when(jdbcTable.dbFieldNames()).thenReturn(Arrays.asList("field1", "field2"));

        H2UpsertQueryBuilder builder = new H2UpsertQueryBuilder(jdbcTable);

        String query = builder.query();
        assertEquals("MERGE INTO table1 (field1,field2) KEY (pk1,pk2 ) VALUES (?, ?)", query);

    }
}
