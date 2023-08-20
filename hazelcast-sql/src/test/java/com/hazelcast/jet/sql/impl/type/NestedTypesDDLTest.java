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

package com.hazelcast.jet.sql.impl.type;

import com.hazelcast.config.Config;
import com.hazelcast.jet.sql.SqlTestSupport;
import com.hazelcast.jet.sql.impl.schema.RelationsStorage;
import com.hazelcast.jet.sql.impl.connector.map.IMapSqlConnector;
import com.hazelcast.test.HazelcastSerialClassRunner;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;

import java.io.Serializable;

import static com.hazelcast.jet.sql.impl.connector.SqlConnector.COMPACT_FORMAT;
import static com.hazelcast.jet.sql.impl.connector.SqlConnector.JAVA_FORMAT;
import static com.hazelcast.jet.sql.impl.connector.SqlConnector.OPTION_FORMAT;
import static com.hazelcast.jet.sql.impl.connector.SqlConnector.OPTION_KEY_CLASS;
import static com.hazelcast.jet.sql.impl.connector.SqlConnector.OPTION_KEY_FORMAT;
import static com.hazelcast.jet.sql.impl.connector.SqlConnector.OPTION_TYPE_COMPACT_TYPE_NAME;
import static com.hazelcast.jet.sql.impl.connector.SqlConnector.OPTION_TYPE_JAVA_CLASS;
import static com.hazelcast.jet.sql.impl.connector.SqlConnector.OPTION_TYPE_PORTABLE_CLASS_ID;
import static com.hazelcast.jet.sql.impl.connector.SqlConnector.OPTION_TYPE_PORTABLE_FACTORY_ID;
import static com.hazelcast.jet.sql.impl.connector.SqlConnector.OPTION_VALUE_CLASS_ID;
import static com.hazelcast.jet.sql.impl.connector.SqlConnector.OPTION_VALUE_CLASS_VERSION;
import static com.hazelcast.jet.sql.impl.connector.SqlConnector.OPTION_VALUE_FACTORY_ID;
import static com.hazelcast.jet.sql.impl.connector.SqlConnector.OPTION_VALUE_FORMAT;
import static com.hazelcast.jet.sql.impl.connector.SqlConnector.PORTABLE_FORMAT;
import static com.hazelcast.spi.properties.ClusterProperty.SQL_CUSTOM_TYPES_ENABLED;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

@RunWith(HazelcastSerialClassRunner.class)
public class NestedTypesDDLTest extends SqlTestSupport {
    private static RelationsStorage storage;

    @BeforeClass
    public static void beforeClass() {
        Config config = smallInstanceConfig()
                .setProperty(SQL_CUSTOM_TYPES_ENABLED.getName(), "true");

        initialize(2, config);
        storage = sqlServiceImpl(instance()).getOptimizer().relationsStorage();
    }

    private static SqlType newJavaType(String name, Class<?> typeClass) {
        return new SqlType(name)
                .options(OPTION_FORMAT, JAVA_FORMAT,
                         OPTION_TYPE_JAVA_CLASS, typeClass.getName());
    }

    private static SqlType newPortableType(String name, int factoryId, int classId) {
        return new SqlType(name)
                .options(OPTION_FORMAT, PORTABLE_FORMAT,
                         OPTION_TYPE_PORTABLE_FACTORY_ID, factoryId,
                         OPTION_TYPE_PORTABLE_CLASS_ID, classId);
    }

    private static SqlType newCompactType(String name, String compactTypeName) {
        return new SqlType(name)
                .options(OPTION_FORMAT, COMPACT_FORMAT,
                         OPTION_TYPE_COMPACT_TYPE_NAME, compactTypeName);
    }

    @Test
    public void test_createTypeIsNotDuplicatedByDefault() {
        newJavaType("FirstType", FirstType.class).create();
        assertThatThrownBy(() -> newJavaType("FirstType", SecondType.class).create())
                .hasMessage("Type already exists: FirstType");
        assertEquals(FirstType.class.getName(), storage.getType("FirstType").getJavaClassName());
    }

    @Test
    public void test_replaceType() {
        newJavaType("FirstType", FirstType.class).create();
        newJavaType("FirstType", SecondType.class).createOrReplace();

        assertEquals(SecondType.class.getName(), storage.getType("FirstType").getJavaClassName());
    }

    @Test
    public void test_createIfNotExists() {
        newJavaType("FirstType", FirstType.class).create();
        newJavaType("FirstType", SecondType.class).createIfNotExists();

        assertEquals(FirstType.class.getName(), storage.getType("FirstType").getJavaClassName());
    }

    @Test
    public void test_showTypes() {
        newJavaType("FirstType", FirstType.class).create();
        newJavaType("SecondType", SecondType.class).create();

        assertRowsAnyOrder("SHOW TYPES", rows(1, "FirstType", "SecondType"));
    }

    @Test
    public void test_dropNonexistentType() {
        assertThatThrownBy(() -> execute("DROP TYPE Foo"))
                .hasMessage("Type does not exist: Foo");

        execute("DROP TYPE IF EXISTS Foo");
    }

    @Test
    public void test_createTwoTypesForSameJavaClass() {
        newJavaType("FirstType", FirstType.class).create();
        assertThatCode(() -> newJavaType("SecondType", FirstType.class).create())
                .doesNotThrowAnyException();
    }

    @Test
    public void test_createTwoTypesForSamePortableClass() {
        newPortableType("FirstType", 123, 456).fields("a INT", "b INT").create();
        newPortableType("SecondType", 123, 456).fields("c VARCHAR", "d VARCHAR").create();

        assertThatCode(() ->
                new SqlMapping("m", IMapSqlConnector.class)
                        .fields("e varchar")
                        .options(OPTION_KEY_FORMAT, JAVA_FORMAT,
                                 OPTION_KEY_CLASS, Long.class.getName(),
                                 OPTION_VALUE_FORMAT, PORTABLE_FORMAT,
                                 OPTION_VALUE_FACTORY_ID, 123,
                                 OPTION_VALUE_CLASS_ID, 456,
                                 OPTION_VALUE_CLASS_VERSION, 0)
                        .createOrReplace())
                .doesNotThrowAnyException();
    }

    @Test
    public void test_createTwoTypesForSameCompactClass() {
        newCompactType("FirstType", "foo").fields("a int", "b varchar").create();
        assertThatCode(() -> newCompactType("SecondType", "foo").fields("a int", "b varchar").create())
                .doesNotThrowAnyException();
    }

    @Test
    public void when_javaClassUnknown_then_fail() {
        assertThatThrownBy(() ->
                new SqlType("FirstType")
                        .options(OPTION_FORMAT, JAVA_FORMAT,
                                 OPTION_TYPE_JAVA_CLASS, "foo")
                        .create())
                .hasRootCauseMessage("foo")
                .hasRootCauseInstanceOf(ClassNotFoundException.class);
    }

    @Test
    public void when_portableClassDefNotKnown_then_requireFields() {
        assertThatThrownBy(() -> newPortableType("FirstType", 123, 456).create())
                .hasMessage("The given FactoryID/ClassID/Version combination not known to the member. You need to provide column list for this type");
    }

    @Test
    public void when_compactTypeNoColumns_then_fail() {
        assertThatThrownBy(() -> newCompactType("FirstType", "foo").create())
                .hasMessage("Column list is required to create Compact-based Types");
    }

    @Test
    public void test_failOnDuplicateColumnName() {
        assertThatThrownBy(() -> newCompactType("TestType", "TestType").fields("id BIGINT", "id BIGINT").create())
                .hasMessageContaining("Column 'id' specified more than once");
    }

    @Test
    public void test_failOnReplaceAndIfNotExists() {
        assertThatThrownBy(() -> execute("CREATE OR REPLACE TYPE IF NOT EXISTS TestType (id BIGINT, name VARCHAR) "
                + "OPTIONS ("
                + "'" + OPTION_FORMAT + "'='" + COMPACT_FORMAT + "',"
                + "'" + OPTION_TYPE_COMPACT_TYPE_NAME + "'='TestType')"))
                .hasMessageContaining("OR REPLACE in conjunction with IF NOT EXISTS not supported");
    }

    @Test
    public void test_fullyQualifiedTypeName() {
        newJavaType("hazelcast.public.FirstType", FirstType.class).create();
        assertNotNull(storage.getType("FirstType"));

        execute("DROP TYPE hazelcast.public.FirstType");
        assertNull(storage.getType("FirstType"));
    }

    @Test
    public void test_failOnNonPublicSchemaType() {
        assertThatThrownBy(() -> newCompactType("information_schema.TestType", "TestType").create())
                .hasMessageContaining("The type must be created in the \"public\" schema");
        assertNull(storage.getType("TestType"));

        newJavaType("hazelcast.public.TestType", FirstType.class).create();
        assertThatThrownBy(() -> execute("DROP TYPE information_schema.TestType"))
                .hasMessageContaining("Type does not exist: information_schema.TestType");
        assertNotNull(storage.getType("TestType"));
    }

    @Test
    public void test_failOnDuplicateOptions() {
        assertThatThrownBy(() -> execute("CREATE TYPE TestType "
                + "OPTIONS ('format'='compact', 'compactTypeName'='TestType', 'compactTypeName'='TestType2')"))
                .hasMessageContaining("Option 'compactTypeName' specified more than once");
    }

    void execute(String sql) {
        instance().getSql().execute(sql);
    }

    public static class FirstType implements Serializable {
        private String name;

        public FirstType() { }

        public FirstType(final String name) {
            this.name = name;
        }

        public String getName() {
            return name;
        }

        public void setName(final String name) {
            this.name = name;
        }
    }

    public static class SecondType implements Serializable {
        private String name;

        public SecondType() { }

        public SecondType(final String name) {
            this.name = name;
        }

        public String getName() {
            return name;
        }

        public void setName(final String name) {
            this.name = name;
        }
    }
}
