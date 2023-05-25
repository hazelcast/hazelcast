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
import com.hazelcast.internal.serialization.impl.portable.DeserializedPortableGenericRecord;
import com.hazelcast.jet.sql.SqlTestSupport;
import com.hazelcast.nio.serialization.ClassDefinition;
import com.hazelcast.nio.serialization.ClassDefinitionBuilder;
import com.hazelcast.sql.HazelcastSqlException;
import com.hazelcast.sql.SqlResult;
import com.hazelcast.sql.SqlRow;
import com.hazelcast.test.HazelcastSerialClassRunner;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;

import java.util.ArrayList;

import static com.hazelcast.spi.properties.ClusterProperty.SQL_CUSTOM_TYPES_ENABLED;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.Assert.assertEquals;

@RunWith(HazelcastSerialClassRunner.class)
public class PortableNestedFieldsTest extends SqlTestSupport {
    @BeforeClass
    public static void beforeClass() {
        Config config = smallInstanceConfig()
                .setProperty(SQL_CUSTOM_TYPES_ENABLED.getName(), "true");

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

    @Test
    public void test_basicQuerying() {
        setupPortableTypesForNestedQuery(instance());
        instance().getSql().execute("INSERT INTO test VALUES (1, 1, 'user1', (1, 'organization1', (1, 'office1')))");

        assertRowsAnyOrder("SELECT (organization).name FROM test", rows(1, "organization1"));
        assertRowsAnyOrder("SELECT (organization).office.name FROM test", rows(1, "office1"));
    }

    @Test
    public void test_nestedPortablesAreReturnedAsDeserialized() {
        instance().getSql().execute("CREATE TYPE Office OPTIONS "
                + "('format'='portable', 'portableFactoryId'='1', 'portableClassId'='3', 'portableClassVersion'='0')");
        instance().getSql().execute("CREATE TYPE Organization OPTIONS "
                + "('format'='portable', 'portableFactoryId'='1', 'portableClassId'='2', 'portableClassVersion'='0')");
        instance().getSql().execute("CREATE TYPE OrganizationAndLong OPTIONS "
                + "('format'='portable', 'portableFactoryId'='1', 'portableClassId'='4', 'portableClassVersion'='0')");

        instance().getSql().execute("CREATE MAPPING test ("
                + "__key BIGINT, "
                + "id BIGINT, "
                + "name VARCHAR, "
                + "organizationAndLong OrganizationAndLong "
                + ") TYPE IMap "
                + "OPTIONS ("
                + "'keyFormat'='bigint', "
                + "'valueFormat'='portable', "
                + "'valuePortableFactoryId'='1', "
                + "'valuePortableClassId'='5')");

        instance().getSql().execute("INSERT INTO test VALUES (1, 1, 'user1', (1, 1, (1, 'organization1', (1, 'office1'))))");

        SqlResult result = instance().getSql().execute("SELECT (organizationAndLong).organization.office FROM test");
        ArrayList<SqlRow> rows = new ArrayList<>();
        for (SqlRow row : result) {
            rows.add(row);
        }
        assertEquals(1, rows.size());
        assertInstanceOf(DeserializedPortableGenericRecord.class, rows.get(0).getObject(0));
    }

    @Test
    public void when_unknownClassDef_noColumns_then_fail() {
        assertThatThrownBy(() -> instance().getSql().execute("CREATE TYPE Foo " +
                        "OPTIONS('format'='portable', 'portableFactoryId'='42', 'portableClassId'='43')"))
                .isInstanceOf(HazelcastSqlException.class)
                .hasMessage("The given FactoryID/ClassID/Version combination not known to the member. You need" +
                        " to provide column list for this type");
    }

    @Test
    public void test_unknownClassDef_givenColumns() {
        instance().getSql().execute("CREATE TYPE Foo (column1 INT, column2 VARCHAR) " +
                "OPTIONS('format'='portable', 'portableFactoryId'='44', 'portableClassId'='45')");
        // we test that the above command doesn't fail.
    }
}
