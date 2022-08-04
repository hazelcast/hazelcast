/*
 * Copyright 2021 Hazelcast Inc.
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

import com.hazelcast.internal.serialization.InternalSerializationService;
import com.hazelcast.internal.serialization.impl.portable.PortableGenericRecordBuilder;
import com.hazelcast.jet.impl.util.Util;
import com.hazelcast.jet.sql.SqlTestSupport;
import com.hazelcast.nio.serialization.ClassDefinition;
import com.hazelcast.nio.serialization.ClassDefinitionBuilder;
import com.hazelcast.nio.serialization.GenericRecord;
import com.hazelcast.sql.HazelcastSqlException;
import com.hazelcast.test.HazelcastSerialClassRunner;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;

import static org.assertj.core.api.Assertions.assertThatThrownBy;

@RunWith(HazelcastSerialClassRunner.class)
public class PortableNestedFieldsTest extends SqlTestSupport {

    private static InternalSerializationService serializationService;
    private static ClassDefinition userType;
    private static ClassDefinition organizationType;
    private static ClassDefinition officeType;

    @BeforeClass
    public static void beforeClass() {
        initialize(2, null);
        serializationService = Util.getSerializationService(instance());
        officeType = new ClassDefinitionBuilder(1, 3)
                .addLongField("id")
                .addStringField("name")
                .build();
        serializationService.getPortableContext().registerClassDefinition(officeType);

        organizationType = new ClassDefinitionBuilder(1, 2)
                .addLongField("id")
                .addStringField("name")
                .addPortableField("office", officeType)
                .build();
        serializationService.getPortableContext().registerClassDefinition(organizationType);

        userType = new ClassDefinitionBuilder(1, 1)
                .addLongField("id")
                .addStringField("name")
                .addPortableField("organization", organizationType)
                .build();
        serializationService.getPortableContext().registerClassDefinition(userType);
    }

    @Test
    public void test_basicQuerying() {
        instance().getSql().execute("CREATE TYPE Office OPTIONS "
                + "('format'='portable', 'portableFactoryId'='1', 'portableClassId'='3', 'portableClassVersion'='0')");
        instance().getSql().execute("CREATE TYPE Organization OPTIONS "
                + "('format'='portable', 'portableFactoryId'='1', 'portableClassId'='2', 'portableClassVersion'='0')");

        instance().getSql().execute("CREATE MAPPING test ("
                + "__key BIGINT, "
                + "id BIGINT, "
                + "name VARCHAR, "
                + "organization Organization "
                + ") TYPE IMap "
                + "OPTIONS ("
                + "'keyFormat'='bigint', "
                + "'valueFormat'='portable', "
                + "'valuePortableFactoryId'='1', "
                + "'valuePortableClassId'='1')");

        final GenericRecord office = new PortableGenericRecordBuilder(officeType)
                .setInt64("id", 1)
                .setString("name", "office1")
                .build();
        final GenericRecord organization = new PortableGenericRecordBuilder(organizationType)
                .setInt64("id", 1)
                .setString("name", "organization1")
                .setGenericRecord("office", office)
                .build();
        instance().getSql().execute("INSERT INTO test (__key, id, name, organization) VALUES (1, 1, 'user1', ?)", organization);

        assertRowsAnyOrder("SELECT (organization).name FROM test", rows(1, "organization1"));
        assertRowsAnyOrder("SELECT (organization).office.name FROM test", rows(1, "office1"));
    }

    @Test
    public void test_portable_unknownClassDef_noColumns() {
        assertThatThrownBy(() -> instance().getSql().execute("CREATE TYPE Foo " +
                        "OPTIONS('format'='portable', 'portableFactoryId'='42', 'portableClassId'='43')"))
                .isInstanceOf(HazelcastSqlException.class)
                .hasMessage("The given FactoryID/ClassID/Version combination not known to the member. You need" +
                        " to provide column list for this type");
    }

    @Test
    public void test_portable_unknownClassDef_givenColumns() {
        instance().getSql().execute("CREATE TYPE Foo (column1 INT, column2 VARCHAR) " +
                "OPTIONS('format'='portable', 'portableFactoryId'='44', 'portableClassId'='45')");
        // we test that the above command doesn't fail.
    }
}
