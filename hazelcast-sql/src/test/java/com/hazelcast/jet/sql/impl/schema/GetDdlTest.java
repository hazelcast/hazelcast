/*
 * Copyright 2023 Hazelcast Inc.
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

package com.hazelcast.jet.sql.impl.schema;

import com.google.common.collect.ImmutableList;
import com.hazelcast.jet.sql.SqlTestSupport;
import com.hazelcast.sql.HazelcastSqlException;
import com.hazelcast.sql.impl.QueryException;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.Collections;

import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class GetDdlTest extends SqlTestSupport {

    @BeforeClass
    public static void beforeClass() throws Exception {
        initialize(1, null);
    }

    @Test
    public void when_queryMappingFromTableNamespace_then_success() {
        createMapping("a", int.class, int.class);

        assertRowsAnyOrder("SELECT GET_DDL('table', 'a')", ImmutableList.of(
                new Row("CREATE MAPPING \"a\" (\n" +
                        "  \"__key\" INTEGER EXTERNAL NAME \"__key\",\n" +
                        "  \"this\" INTEGER EXTERNAL NAME \"this\"\n" +
                        ")\n" +
                        "TYPE IMap\n" +
                        "OPTIONS (\n" +
                        "  'keyFormat' = 'java',\n" +
                        "  'keyJavaClass' = 'int',\n" +
                        "  'valueFormat' = 'java',\n" +
                        "  'valueJavaClass' = 'int'\n" +
                        ")"))
        );
    }

    @Test
    public void when_queryViewFromTableNamespace_then_success() {
        createMapping("a", int.class, int.class);
        instance().getSql().execute("CREATE VIEW v AS SELECT * FROM a");

        assertRowsAnyOrder("SELECT GET_DDL('table', 'v')", ImmutableList.of(
                new Row("CREATE VIEW  \"v\" AS\n" +
                        "SELECT \"a\".\"__key\", \"a\".\"this\"\n" +
                        "FROM \"hazelcast\".\"public\".\"a\" AS \"a\""))
        );
    }

    @

            Test
    public void when_queryNotSupportedNamespace_then_throws() {
        assertThatThrownBy(() -> instance().getSql().execute("SELECT GET_DDL('a', 'b')"))
                .hasCauseInstanceOf(QueryException.class)
                .hasMessageContaining("Namespace 'a' is not supported.");
    }

    @Test
    public void when_queryNonExistingObject_then_throws() {
        assertThatThrownBy(() -> instance().getSql().execute("SELECT GET_DDL('table', 'bbb')"))
                .hasCauseInstanceOf(QueryException.class)
                .hasMessageContaining("Object 'bbb' does not exist");
    }
}
