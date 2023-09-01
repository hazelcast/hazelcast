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
import com.hazelcast.internal.serialization.impl.compact.DeserializedGenericRecordBuilder;
import com.hazelcast.internal.serialization.impl.portable.PortableGenericRecordBuilder;
import com.hazelcast.jet.sql.SqlTestSupport;
import com.hazelcast.jet.sql.impl.connector.SqlConnector;
import com.hazelcast.jet.sql.impl.connector.map.IMapSqlConnector;
import com.hazelcast.nio.serialization.ClassDefinition;
import com.hazelcast.nio.serialization.ClassDefinitionBuilder;
import com.hazelcast.test.HazelcastParametrizedRunner;
import com.hazelcast.test.HazelcastSerialParametersRunnerFactory;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized.Parameter;
import org.junit.runners.Parameterized.Parameters;
import org.junit.runners.Parameterized.UseParametersRunnerFactory;

import java.io.Serializable;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

import static com.hazelcast.jet.sql.impl.connector.SqlConnector.COMPACT_FORMAT;
import static com.hazelcast.jet.sql.impl.connector.SqlConnector.JAVA_FORMAT;
import static com.hazelcast.jet.sql.impl.connector.SqlConnector.OPTION_KEY_FORMAT;
import static com.hazelcast.jet.sql.impl.connector.SqlConnector.OPTION_VALUE_CLASS;
import static com.hazelcast.jet.sql.impl.connector.SqlConnector.OPTION_VALUE_CLASS_ID;
import static com.hazelcast.jet.sql.impl.connector.SqlConnector.OPTION_VALUE_COMPACT_TYPE_NAME;
import static com.hazelcast.jet.sql.impl.connector.SqlConnector.OPTION_VALUE_FACTORY_ID;
import static com.hazelcast.jet.sql.impl.connector.SqlConnector.OPTION_VALUE_FORMAT;
import static com.hazelcast.jet.sql.impl.connector.SqlConnector.PORTABLE_FORMAT;
import static com.hazelcast.spi.properties.ClusterProperty.SQL_CUSTOM_TYPES_ENABLED;

@RunWith(HazelcastParametrizedRunner.class)
@UseParametersRunnerFactory(HazelcastSerialParametersRunnerFactory.class)
public class NestedTypesSerializationTest extends SqlTestSupport {
    private static final ClassDefinition PORTABLE_ORG_SCHEMA = new ClassDefinitionBuilder(1, 2)
            .addStringField("name")
            .addStringField("taxId")
            .addBooleanField("paidCustomer")
            .build();
    private static final ClassDefinition PORTABLE_USER_SCHEMA = new ClassDefinitionBuilder(1, 1)
            .addStringField("name")
            .addStringField("phone")
            .addPortableField("organization", PORTABLE_ORG_SCHEMA)
            .build();

    private static final Map<String, Object> ORG_VALUES = Map.of(
            "Portable", new PortableGenericRecordBuilder(PORTABLE_ORG_SCHEMA)
                    .setString("name", "Bell Labs")
                    .setString("taxId", "1234567890")
                    .setBoolean("paidCustomer", true)
                    .build(),
            "Compact", new DeserializedGenericRecordBuilder("Organization")
                    .setString("name", "Bell Labs")
                    .setString("taxId", "1234567890")
                    .setBoolean("paidCustomer", true)
                    .build(),
            "Java", new Organization("Bell Labs", "1234567890", true)
    );

    @Parameters(name = "{0} -> {1}")
    public static Iterable<Object[]> parameters() {
        Map<String, Function<String, SqlMapping>> mappings = Map.of(
                "Portable", name -> mapping(name, IMapSqlConnector.class)
                        .options(OPTION_VALUE_FORMAT, PORTABLE_FORMAT,
                                 OPTION_VALUE_FACTORY_ID, PORTABLE_USER_SCHEMA.getFactoryId(),
                                 OPTION_VALUE_CLASS_ID, PORTABLE_USER_SCHEMA.getClassId()),
                "Compact", name -> mapping(name, IMapSqlConnector.class)
                        .options(OPTION_VALUE_FORMAT, COMPACT_FORMAT,
                                 OPTION_VALUE_COMPACT_TYPE_NAME, "User"),
                "Java", name -> mapping(name, IMapSqlConnector.class)
                        .options(OPTION_VALUE_FORMAT, JAVA_FORMAT,
                                 OPTION_VALUE_CLASS, User.class.getName())
        );
        return parameters((String) null, mappings, mappings);
    }

    private static SqlMapping mapping(String name, Class<? extends SqlConnector> connector) {
        return new SqlMapping(name, connector)
                .fields("__key INT",
                        "name VARCHAR",
                        "organization Organization")
                .options(OPTION_KEY_FORMAT, "int");
    }

    @Parameter(0)
    public String fromType;
    @Parameter(1)
    public String toType;

    @Parameter(2)
    public Function<String, SqlMapping> getFromMapping;
    @Parameter(3)
    public Function<String, SqlMapping> getToMapping;

    @BeforeClass
    public static void setup() throws Exception {
        Config config = smallInstanceConfig()
                .setProperty(SQL_CUSTOM_TYPES_ENABLED.getName(), "true");

        SerializationConfig serializationConfig = config.getSerializationConfig();
        serializationConfig.addClassDefinition(PORTABLE_ORG_SCHEMA);
        serializationConfig.addClassDefinition(PORTABLE_USER_SCHEMA);

        initializeWithClient(2, config, null);
    }

    @Test
    public void test_insertSelect() {
        new SqlType("Organization").fields("name VARCHAR", "paidCustomer BOOLEAN").create();

        String from = randomName();
        getFromMapping.apply(from).create();

        // Insert using row syntax (...)
        insertRow(client(), from, 1, "Alice", row("Umbrella Corporation", false));

        // Insert using dynamic parameters (?)
        insertRecord(client(), from, 2, "Dennis", ORG_VALUES.get(fromType));

        String to = randomName();
        getToMapping.apply(to).create();

        instance().getSql().execute("INSERT INTO " + to + " SELECT * FROM " + from);

        assertRowsEventuallyInAnyOrder(
                "SELECT name, (organization).name, (organization).paidCustomer FROM " + to,
                List.of(
                        new Row("Alice", "Umbrella Corporation", false),
                        new Row("Dennis", "Bell Labs", true)
                )
        );
    }

    public static class Organization implements Serializable {
        public String name;
        public String taxId;
        public Boolean paidCustomer;

        public Organization() { }

        public Organization(String name, String taxId, Boolean paidCustomer) {
            this.name = name;
            this.taxId = taxId;
            this.paidCustomer = paidCustomer;
        }
    }

    public static class User implements Serializable {
        public String name;
        public String phone;
        public Organization organization;

        public User() { }

        public User(String name, String phone, Organization organization) {
            this.name = name;
            this.phone = phone;
            this.organization = organization;
        }
    }
}
