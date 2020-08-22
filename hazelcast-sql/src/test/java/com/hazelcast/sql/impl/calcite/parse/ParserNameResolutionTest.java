/*
 * Copyright (c) 2008-2020, Hazelcast, Inc. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.hazelcast.sql.impl.calcite.parse;

import com.hazelcast.sql.impl.SqlErrorCode;
import com.hazelcast.sql.impl.QueryException;
import com.hazelcast.sql.impl.QueryUtils;
import com.hazelcast.sql.impl.calcite.HazelcastSqlBackend;
import com.hazelcast.sql.impl.calcite.OptimizerContext;
import com.hazelcast.sql.impl.calcite.TestMapTable;
import com.hazelcast.sql.impl.calcite.TestTableResolver;
import com.hazelcast.sql.impl.schema.SqlCatalog;
import com.hazelcast.sql.impl.schema.TableResolver;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.SqlSelect;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static com.hazelcast.sql.impl.QueryUtils.CATALOG;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * Tests for object name resolution during parsing.
 */
@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class ParserNameResolutionTest {

    private static final String BAD_CATALOG = "badCatalog";

    private static final String SCHEMA_1 = "mySchema1";
    private static final String SCHEMA_2 = "mySchema2";
    private static final String BAD_SCHEMA = "badSchema";

    private static final String TABLE_1 = "myTable1";
    private static final String TABLE_2 = "myTable2";
    private static final String BAD_TABLE = "badTable";

    private static final String FIELD_1 = "myField1";
    private static final String FIELD_2 = "myField2";
    private static final String BAD_FIELD = "badField";

    private static final String TABLE_1_FQN = SqlIdentifier.getString(Arrays.asList(CATALOG, SCHEMA_1, TABLE_1));
    private static final String TABLE_2_FQN = SqlIdentifier.getString(Arrays.asList(CATALOG, SCHEMA_2, TABLE_2));

    @Test
    public void testNameResolution() {
        // Lookup for table without default path.
        checkFailure(errorObjectNotFound(TABLE_1), FIELD_1, TABLE_1);
        checkSuccess(FIELD_1, TABLE_1_FQN, SCHEMA_1, TABLE_1);
        checkSuccess(FIELD_1, TABLE_1_FQN, CATALOG, SCHEMA_1, TABLE_1);

        // Lookup for table with default path.
        checkSuccess(FIELD_2, TABLE_2_FQN, TABLE_2);
        checkSuccess(FIELD_2, TABLE_2_FQN, SCHEMA_2, TABLE_2);
        checkSuccess(FIELD_2, TABLE_2_FQN, CATALOG, SCHEMA_2, TABLE_2);

        // Test overridden search path.
        checkSuccess(createContext(SCHEMA_1), FIELD_1, TABLE_1_FQN, TABLE_1);

        // Wrong field
        checkFailure(errorColumnNotFound(BAD_FIELD), BAD_FIELD, SCHEMA_1, TABLE_1);

        // Wrong table
        checkFailure(errorObjectNotFound(BAD_TABLE), BAD_FIELD, BAD_TABLE);

        // Wrong table in existing schema
        checkFailure(errorObjectNotFoundWithin(BAD_TABLE, CATALOG, SCHEMA_1), BAD_FIELD, SCHEMA_1, BAD_TABLE);

        // Wrong table in existing schema/catalog
        checkFailure(errorObjectNotFoundWithin(BAD_TABLE, CATALOG, SCHEMA_1), BAD_FIELD, CATALOG, SCHEMA_1, BAD_TABLE);

        // Wrong schema
        checkFailure(errorObjectNotFound(BAD_SCHEMA), BAD_FIELD, BAD_SCHEMA, BAD_TABLE);

        // Wrong schema in existing catalog
        checkFailure(errorObjectNotFoundWithin(BAD_SCHEMA, CATALOG), BAD_FIELD, CATALOG, BAD_SCHEMA, BAD_TABLE);

        // Wrong catalog
        checkFailure(errorObjectNotFound(BAD_CATALOG), BAD_FIELD, BAD_CATALOG, BAD_SCHEMA, BAD_TABLE);
    }

    private static void checkSuccess(String fieldName, String tableFqn, String... tableComponents) {
        checkSuccess(createContext(), fieldName, tableFqn, tableComponents);
    }

    private static void checkSuccess(OptimizerContext context, String fieldName, String tableFqn, String... tableComponents) {
        QueryParseResult res = context.parse(composeSelect(fieldName, tableComponents));

        SqlSelect select = (SqlSelect) res.getNode();

        SqlNodeList selectList = select.getSelectList();
        assertEquals(1, selectList.size());

        SqlIdentifier fieldIdentifier = (SqlIdentifier) selectList.get(0);
        assertEquals(SqlIdentifier.getString(Arrays.asList(last(tableComponents), fieldName)), fieldIdentifier.toString());

        SqlCall from = (SqlCall) select.getFrom();
        assertEquals(from.getKind(), SqlKind.AS);
        assertEquals(tableFqn, from.operand(0).toString());
        assertEquals(last(tableComponents), from.operand(1).toString());
    }

    private static void checkFailure(String errorMessage, String fieldName, String... tableComponents) {
        try {
            createContext().parse(composeSelect(fieldName, tableComponents));

            fail();
        } catch (QueryException e) {
            assertEquals(SqlErrorCode.PARSING, e.getCode());

            Throwable cause = e.getCause();
            assertTrue(cause.getMessage(), cause.getMessage().endsWith(errorMessage));
        }
    }

    private static String errorColumnNotFound(String columnName) {
        return "Column '" + columnName + "' not found in any table";
    }

    private static String errorObjectNotFound(String tableName) {
        return "Object '" + tableName + "' not found";
    }

    private static String errorObjectNotFoundWithin(String tableName, String... schemaComponents) {
        return errorObjectNotFound(tableName) + " within '" + SqlIdentifier.getString(Arrays.asList(schemaComponents)) + "'";
    }

    private static String composeSelect(String fieldName, String... tableComponents) {
        StringBuilder res = new StringBuilder("SELECT " + fieldName + " FROM ");

        boolean first = true;

        for (String tableComponent : tableComponents) {
            if (first) {
                first = false;
            } else {
                res.append(".");
            }

            res.append(tableComponent);
        }

        return res.toString();
    }

    private static OptimizerContext createContext() {
        return createContext(null);
    }

    private static OptimizerContext createContext(String additionalSearchPath) {
        TableResolver resolverWithoutSearchPath = TestTableResolver.create(
            TestMapTable.create(SCHEMA_1, TABLE_1, TestMapTable.field(FIELD_1))
        );

        TableResolver resolverWithSearchPath = TestTableResolver.create(
            SCHEMA_2,
            TestMapTable.create(SCHEMA_2, TABLE_2, TestMapTable.field(FIELD_2))
        );

        List<TableResolver> tableResolvers = Arrays.asList(resolverWithoutSearchPath, resolverWithSearchPath);

        List<List<String>> additionalSearchPaths = additionalSearchPath != null
            ? Collections.singletonList(Arrays.asList(CATALOG, additionalSearchPath)) : Collections.emptyList();

        List<List<String>> searchPaths = QueryUtils.prepareSearchPaths(
            additionalSearchPaths,
            tableResolvers
        );

        return OptimizerContext.create(
            new SqlCatalog(tableResolvers),
            searchPaths,
            1,
            new HazelcastSqlBackend(null),
            null
        );
    }

    private static <E> E last(E[] array) {
        return array[array.length - 1];
    }

}
