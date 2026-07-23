/*
 * Copyright 2026 Hazelcast Inc.
 *
 * Licensed under the Hazelcast Community License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://hazelcast.com/hazelcast-community-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.hazelcast.jet.sql.impl.connector.jdbc.mssql;

import com.hazelcast.sql.impl.type.QueryDataType;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.apache.calcite.sql.SqlDialect;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static org.junit.Assert.assertSame;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class HazelcastMSSQLDialectTest {

    private final HazelcastMSSQLDialect dialect = new HazelcastMSSQLDialect(SqlDialect.EMPTY_CONTEXT);

    @Test
    public void resolveType_whenIntIdentity_thenReturnsInt() {
        assertSame(QueryDataType.INT, dialect.resolveType("int identity", 0, 0));
    }

    @Test
    public void resolveType_whenNvarchar_thenReturnsVarchar() {
        assertSame(QueryDataType.VARCHAR, dialect.resolveType("nvarchar", 0, 0));
    }
}
