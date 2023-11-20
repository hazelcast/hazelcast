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

import com.hazelcast.jet.sql.SqlTestSupport;
import com.hazelcast.jet.sql.impl.schema.RelationsStorage;
import com.hazelcast.jet.sql.impl.type.BasicNestedFieldsTest.RegularPOJO;
import com.hazelcast.test.HazelcastSerialClassRunner;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;

import static com.hazelcast.jet.sql.impl.type.BasicNestedFieldsTest.createJavaMapping;
import static com.hazelcast.jet.sql.impl.type.CompactNestedFieldsTest.createCompactMapping;
import static com.hazelcast.sql.impl.type.QueryDataType.INT;
import static com.hazelcast.sql.impl.type.QueryDataType.VARCHAR;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

@RunWith(HazelcastSerialClassRunner.class)
public class NestedTypesDDLTest extends SqlTestSupport {
    private static RelationsStorage storage;

    @BeforeClass
    public static void beforeClass() {
        initialize(2, null);
        storage = sqlServiceImpl(instance()).getOptimizer().relationsStorage();
    }

    @Test
    public void test_createTypeIsNotDuplicatedByDefault() {
        new SqlType("FirstType").create();
        assertThatThrownBy(() -> new SqlType("FirstType").create())
                .hasMessage("Type already exists: FirstType");
    }

    @Test
    public void test_replaceType() {
        new SqlType("FirstType").fields("a INT").create();
        new SqlType("FirstType").fields("a VARCHAR").createOrReplace();

        assertEquals(VARCHAR, storage.getType("FirstType").getFields().get(0).getType());
    }

    @Test
    public void test_createIfNotExists() {
        new SqlType("FirstType").fields("a INT").create();
        new SqlType("FirstType").fields("a VARCHAR").createIfNotExists();

        assertEquals(INT, storage.getType("FirstType").getFields().get(0).getType());
    }

    @Test
    public void test_showTypes() {
        new SqlType("FirstType").create();
        new SqlType("SecondType").create();

        assertRowsAnyOrder("SHOW TYPES", rows(1, "FirstType", "SecondType"));
    }

    @Test
    public void test_dropNonexistentType() {
        assertThatThrownBy(() -> execute("DROP TYPE Foo"))
                .hasMessage("Type does not exist: Foo");

        execute("DROP TYPE IF EXISTS Foo");
    }

    @Test
    public void when_compactTypeNoColumns_then_fail() {
        new SqlType("FirstType").create();
        assertThatThrownBy(() -> createCompactMapping(instance(), "test", "foo", "bar FirstType"))
                .hasMessage("Column list is required to create Compact-based types");
    }

    @Test
    public void test_typeFieldResolution() {
        new SqlType("NestedType").create();
        assertTrue(storage.getType("NestedType").getFields().isEmpty());
        assertThatThrownBy(() -> createCompactMapping(instance(), "test", "foo", "bar NestedType"))
                .hasMessage("Column list is required to create Compact-based types");

        createJavaMapping(instance(), "test2", RegularPOJO.class, "name VARCHAR", "child NestedType");
        assertFalse(storage.getType("NestedType").getFields().isEmpty());
        assertThatCode(() -> createCompactMapping(instance(), "test", "foo", "bar NestedType"))
                .doesNotThrowAnyException();
    }

    @Test
    public void test_failOnDuplicateColumnName() {
        assertThatThrownBy(() -> new SqlType("TestType").fields("id BIGINT", "id BIGINT").create())
                .hasMessageContaining("Column 'id' specified more than once");
    }

    @Test
    public void test_failOnReplaceAndIfNotExists() {
        assertThatThrownBy(() -> execute("CREATE OR REPLACE TYPE IF NOT EXISTS TestType (id BIGINT, name VARCHAR)"))
                .hasMessageContaining("OR REPLACE in conjunction with IF NOT EXISTS not supported");
    }

    @Test
    public void test_fullyQualifiedTypeName() {
        new SqlType("hazelcast.public.FirstType").create();
        assertNotNull(storage.getType("FirstType"));

        execute("DROP TYPE hazelcast.public.FirstType");
        assertNull(storage.getType("FirstType"));
    }

    @Test
    public void test_failOnNonPublicSchemaType() {
        assertThatThrownBy(() -> new SqlType("information_schema.TestType").create())
                .hasMessageContaining("The type must be created in the \"public\" schema");
        assertNull(storage.getType("TestType"));

        new SqlType("hazelcast.public.TestType").create();
        assertThatThrownBy(() -> execute("DROP TYPE information_schema.TestType"))
                .hasMessageContaining("Type does not exist: information_schema.TestType");
        assertNotNull(storage.getType("TestType"));
    }

    void execute(String sql) {
        instance().getSql().execute(sql);
    }
}
