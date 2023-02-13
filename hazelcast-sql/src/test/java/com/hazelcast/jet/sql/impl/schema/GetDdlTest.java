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
import com.hazelcast.config.Config;
import com.hazelcast.jet.sql.SqlTestSupport;
import com.hazelcast.sql.SqlResult;
import com.hazelcast.sql.impl.QueryException;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.Arrays;

import static com.hazelcast.spi.properties.ClusterProperty.SQL_CUSTOM_TYPES_ENABLED;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class GetDdlTest extends SqlTestSupport {

    @BeforeClass
    public static void beforeClass() throws Exception {
        Config config = smallInstanceConfig()
                .setProperty(SQL_CUSTOM_TYPES_ENABLED.getName(), "true");

        initialize(1, config);
    }

    @Test
    public void when_queryMappingFromTableNamespace_then_success() {
        createMapping("a", int.class, int.class);

        assertRowsAnyOrder("SELECT GET_DDL('table', 'a')", ImmutableList.of(
                new Row("CREATE MAPPING \"a\" " +
                        "(__key INTEGER EXTERNAL NAME __key, this INTEGER EXTERNAL NAME this) \n" +
                        "TYPE IMap \n" +
                        "OPTIONS( \n" +
                        "'keyFormat' = 'java',\n" +
                        "'keyJavaClass' = 'int',\n" +
                        "'valueFormat' = 'java',\n" +
                        "'valueJavaClass' = 'int')\n"))
        );
    }

    @Test
    public void when_queryViewFromTableNamespace_then_success() {
        createMapping("a", int.class, int.class);
        instance().getSql().execute("CREATE VIEW v AS SELECT * FROM a");

        assertRowsAnyOrder("SELECT GET_DDL('table', 'v')", ImmutableList.of(
                new Row("CREATE VIEW \"v\" AS\n" +
                        "SELECT \"a\".\"__key\", \"a\".\"this\"\n" +
                        "FROM \"hazelcast\".\"public\".\"a\" AS \"a\""))
        );
    }

    @Test
    public void when_queryTypeFromTableNamespace_then_success() {
        String createTypeQuery = "CREATE TYPE \"t\" (a INTEGER, b INTEGER) OPTIONS (\n"
                + "'format' = 'portable', 'portableFactoryId' = '1', "
                + "'portableClassId' = '3', 'portableClassVersion' = '0')\n";

        instance().getSql().execute(createTypeQuery);
        assertRowsAnyOrder("SELECT GET_DDL('table', 't')", ImmutableList.of(new Row(createTypeQuery)));
    }

    @Test
    public void when_queryNotSupportedNamespace_then_throws() {
        SqlResult sqlRows = instance().getSql().execute("SELECT GET_DDL('a', 'b')");

        assertThatThrownBy(() -> sqlRows.iterator().next())
                .hasCauseInstanceOf(QueryException.class)
                .hasMessageContaining("Namespace 'a' is not supported.");
    }

    @Test
    public void when_queryNonExistingObject_then_throws() {
        SqlResult sqlRows = instance().getSql().execute("SELECT GET_DDL('table', 'bbb')");
        assertThatThrownBy(() -> sqlRows.iterator().next())
                .hasCauseInstanceOf(QueryException.class)
                .hasMessageContaining("Object 'bbb' does not exist in namespace 'table'");
    }

    @Test
    public void when_queryDdlWithAnotherOperator_then_success() {
        createMapping("a", int.class, int.class);

        assertRowsAnyOrder("SELECT SUBSTRING(GET_DDL('table', 'a') FROM 1 FOR 6)",
                ImmutableList.of(new Row("CREATE"))
        );

        assertRowsAnyOrder(
                "SELECT SUBSTRING(GET_DDL('table', 'a') FROM 1 FOR 6) " +
                        "|| SUBSTRING(GET_DDL('table', 'a') FROM 1 FOR 3)",
                ImmutableList.of(new Row("CREATECRE"))
        );
    }

    @Test
    public void when_queryDdlWithOtherRels_then_success() {
        createMapping("a", int.class, int.class);

        assertRowsAnyOrder("SELECT SUBSTRING(GET_DDL('table', 'a') FROM 1 FOR 6)" +
                        "UNION ALL SELECT SUBSTRING(GET_DDL('table', 'a') FROM 1 FOR 6)",
                Arrays.asList(new Row("CREATE"), new Row("CREATE")));
    }

    @Test
    public void when_queryDdlWithInput_then_success() {
        createMapping("a", int.class, String.class);
        instance().getMap("a").put(1, "a");

        assertRowsAnyOrder("SELECT GET_DDL('table', this) FROM a",
                ImmutableList.of(new Row("CREATE MAPPING \"a\" " +
                        "(__key INTEGER EXTERNAL NAME __key, this VARCHAR EXTERNAL NAME this) \n" +
                        "TYPE IMap \n" +
                        "OPTIONS( \n" +
                        "'keyFormat' = 'java',\n" +
                        "'keyJavaClass' = 'int',\n" +
                        "'valueFormat' = 'java',\n" +
                        "'valueJavaClass' = 'java.lang.String')\n")));
    }
}
