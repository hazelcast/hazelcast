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
import com.hazelcast.config.SerializationConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.internal.serialization.impl.portable.DeserializedPortableGenericRecord;
import com.hazelcast.jet.sql.SqlTestSupport;
import com.hazelcast.jet.sql.impl.connector.map.IMapSqlConnector;
import com.hazelcast.nio.serialization.ClassDefinition;
import com.hazelcast.nio.serialization.ClassDefinitionBuilder;
import com.hazelcast.sql.SqlResult;
import com.hazelcast.sql.SqlRow;
import com.hazelcast.test.HazelcastSerialClassRunner;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;

import java.util.ArrayList;

import static com.hazelcast.jet.sql.impl.connector.SqlConnector.OPTION_KEY_FORMAT;
import static com.hazelcast.jet.sql.impl.connector.SqlConnector.OPTION_VALUE_CLASS_ID;
import static com.hazelcast.jet.sql.impl.connector.SqlConnector.OPTION_VALUE_FACTORY_ID;
import static com.hazelcast.jet.sql.impl.connector.SqlConnector.OPTION_VALUE_FORMAT;
import static com.hazelcast.jet.sql.impl.connector.SqlConnector.PORTABLE_FORMAT;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.junit.Assert.assertEquals;

@RunWith(HazelcastSerialClassRunner.class)
public class PortableNestedFieldsTest extends SqlTestSupport {

    @BeforeClass
    public static void beforeClass() {
        Config config = smallInstanceConfig();

        SerializationConfig serializationConfig = config.getSerializationConfig();

        ClassDefinition officeType = new ClassDefinitionBuilder(1, 3)
                .addLongField("id")
                .addStringField("name")
                .build();
        serializationConfig.addClassDefinition(officeType);

        ClassDefinition organizationType = new ClassDefinitionBuilder(1, 2)
                .addLongField("id")
                .addStringField("name")
                .addPortableField("office", officeType)
                .build();
        serializationConfig.addClassDefinition(organizationType);

        ClassDefinition organizationAndLongType = new ClassDefinitionBuilder(1, 4)
                .addLongField("id")
                .addLongField("l")
                .addPortableField("organization", organizationType)
                .build();
        serializationConfig.addClassDefinition(organizationAndLongType);

        ClassDefinition userType = new ClassDefinitionBuilder(1, 1)
                .addLongField("id")
                .addStringField("name")
                .addPortableField("organization", organizationType)
                .build();
        serializationConfig.addClassDefinition(userType);

        ClassDefinition userType2 = new ClassDefinitionBuilder(1, 5)
                .addLongField("id")
                .addStringField("name")
                .addPortableField("organizationAndLong", organizationAndLongType)
                .build();
        serializationConfig.addClassDefinition(userType2);

        initialize(2, config);
    }

    static void setupPortableTypesForNestedQuery(HazelcastInstance instance) {
        new SqlType("Office").create(instance);
        new SqlType("Organization").fields("id BIGINT", "name VARCHAR", "office Office").create(instance);

        createPortableMapping(instance, "test", 1, 1, "id BIGINT", "name VARCHAR", "organization Organization");
    }

    static void createPortableMapping(HazelcastInstance instance, String name, int factoryId, int classId,
                                      String... valueFields) {
        new SqlMapping(name, IMapSqlConnector.class)
                .fields("__key BIGINT")
                .fields(valueFields)
                .options(OPTION_KEY_FORMAT, "bigint",
                         OPTION_VALUE_FORMAT, PORTABLE_FORMAT,
                         OPTION_VALUE_FACTORY_ID, factoryId,
                         OPTION_VALUE_CLASS_ID, classId)
                .create(instance);
    }

    private static SqlResult execute(String sql) {
        return instance().getSql().execute(sql);
    }

    @Test
    public void test_basicQuerying() {
        setupPortableTypesForNestedQuery(instance());
        execute("INSERT INTO test VALUES (1, 1, 'user1', (1, 'organization1', (1, 'office1')))");

        assertRowsAnyOrder("SELECT (organization).name FROM test", rows(1, "organization1"));
        assertRowsAnyOrder("SELECT (organization).office.name FROM test", rows(1, "office1"));
    }

    @Test
    public void test_nestedPortablesAreReturnedAsDeserialized() {
        new SqlType("Office").create();
        new SqlType("Organization").fields("id BIGINT", "name VARCHAR", "office Office").create();
        new SqlType("OrganizationAndLong").fields("id BIGINT", "l BIGINT", "organization Organization").create();

        createPortableMapping(instance(), "test", 1, 5,
                "id BIGINT", "name VARCHAR", "organizationAndLong OrganizationAndLong");

        execute("INSERT INTO test VALUES (1, 1, 'user1', (1, 1, (1, 'organization1', (1, 'office1'))))");

        SqlResult result = execute("SELECT (organizationAndLong).organization.office FROM test");
        ArrayList<SqlRow> rows = new ArrayList<>();
        for (SqlRow row : result) {
            rows.add(row);
        }
        assertEquals(1, rows.size());
        assertInstanceOf(DeserializedPortableGenericRecord.class, rows.get(0).getObject(0));
    }

    @Test
    public void test_unknownClassDef_givenColumns() {
        assertThatCode(() -> new SqlType("Foo").fields("column1 INT", "column2 VARCHAR").create())
                .doesNotThrowAnyException();
    }
}
