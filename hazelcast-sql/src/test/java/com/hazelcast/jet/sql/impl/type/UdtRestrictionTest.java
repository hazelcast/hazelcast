/*
 * Copyright 2024 Hazelcast Inc.
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

import com.hazelcast.config.JavaSerializationFilterConfig;
import com.hazelcast.jet.sql.SqlTestSupport;
import com.hazelcast.jet.sql.impl.connector.map.IMapSqlConnector;
import com.hazelcast.sql.SqlService;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.Serializable;

import static com.hazelcast.jet.sql.impl.connector.SqlConnector.JAVA_FORMAT;
import static com.hazelcast.jet.sql.impl.connector.SqlConnector.OPTION_KEY_CLASS;
import static com.hazelcast.jet.sql.impl.connector.SqlConnector.OPTION_KEY_FORMAT;
import static com.hazelcast.jet.sql.impl.connector.SqlConnector.OPTION_VALUE_CLASS;
import static com.hazelcast.jet.sql.impl.connector.SqlConnector.OPTION_VALUE_FORMAT;
import static org.assertj.core.api.Assertions.assertThatNoException;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class UdtRestrictionTest extends SqlTestSupport {
    static SqlService sqlService;

    private static final Class<AllowedClass> ALLOWED_CLASS = AllowedClass.class;

    private static final Class<NotAllowedClass> DENIED_CLASS = NotAllowedClass.class;


    private final String expectedMessage = String.format("Creation of class %s is not allowed.", DENIED_CLASS.getName());

    @BeforeClass
    public static void setup() {
        var config = smallInstanceConfig();
        JavaSerializationFilterConfig reflectionConfig = new JavaSerializationFilterConfig();
        reflectionConfig.setDefaultsDisabled(true);
        reflectionConfig.getBlacklist().addClasses(DENIED_CLASS.getName());
        reflectionConfig.getWhitelist().addClasses(ALLOWED_CLASS.getName());
        config.getSqlConfig().setJavaReflectionFilterConfig(reflectionConfig);
        initialize(2, config);
        sqlService = instance().getSql();
    }

    @Test
    public void when_valueClassRestricted_then_fails() {
        String name = "map";
        javaMapping(name, ALLOWED_CLASS, DENIED_CLASS).create();

        assertThatThrownBy(() -> execute("SINK INTO " + name + " VALUES (1, 1)"))
            .hasMessageContaining(expectedMessage);
        assertThatThrownBy(() -> execute("INSERT INTO " + name + " VALUES (2, 2)"))
            .hasMessageContaining(expectedMessage);
        assertThatThrownBy(() -> execute("SINK INTO " + name + " VALUES (1, null)"))
            .hasMessageContaining(expectedMessage);
        assertThatThrownBy(() -> execute("INSERT INTO " + name + " VALUES (1, null)"))
            .hasMessageContaining(expectedMessage);
    }

    @Test
    public void when_keyClassRestricted_then_fails() {
        String name = "map";
        javaMapping(name, DENIED_CLASS, ALLOWED_CLASS).create();

        assertThatThrownBy(() -> execute("SINK INTO " + name + " VALUES (1, 1)"))
            .hasMessageContaining(expectedMessage);
        assertThatThrownBy(() -> execute("INSERT INTO " + name + " VALUES (1, 2)"))
            .hasMessageContaining(expectedMessage);
        assertThatThrownBy(() -> execute("SINK INTO " + name + " VALUES (null, 1)"))
            .hasMessageContaining(expectedMessage);
        assertThatThrownBy(() -> execute("INSERT INTO " + name + " VALUES (null, 2)"))
            .hasMessageContaining(expectedMessage);

    }

    @Test
    public void when_valueClassAllowed_then_success() {
        String name = "map";
        javaMapping(name, Integer.class, ALLOWED_CLASS).create();
        assertThatNoException().isThrownBy(() -> execute("SINK INTO " + name + " VALUES (1, 1)"));
        assertThatNoException().isThrownBy(() -> execute("INSERT INTO " + name + " VALUES (2, 2)"));
        assertThatNoException().isThrownBy(() -> execute("SINK INTO " + name + " VALUES (3, null)"));
        assertThatNoException().isThrownBy(() -> execute("INSERT INTO " + name + " VALUES (4, null)"));
    }

    @Test
    public void when_keyClassAllowed_then_success() {
        String name = "map";
        javaMapping(name, AllowedClass.class, Integer.class).create();
        assertThatNoException().isThrownBy(() -> execute("SINK INTO " + name + " VALUES (1, 1)"));
        assertThatNoException().isThrownBy(() -> execute("INSERT INTO " + name + " VALUES (2, 2)"));
        assertThatNoException().isThrownBy(() -> execute("INSERT INTO " + name + " VALUES (null, 3)"));
        assertThatNoException().isThrownBy(() -> execute("SINK INTO " + name + " VALUES (null, 2)"));

    }

    @Test
    public void when_updateValueWithRestrictedClass_then_failed() {
        String name = "map";
        javaMapping(name, Integer.class, NotAllowedClass.class).create();
        instance().getMap(name).put(1, "initial");

        assertThatThrownBy(() -> execute("UPDATE map SET restricted = 100 WHERE __key = 1"))
            .hasMessageContaining(expectedMessage);

        assertThatThrownBy(() -> execute("UPDATE map SET restricted = null WHERE __key = 1"))
            .hasMessageContaining(expectedMessage);
    }

    @Test
    public void when_updateValueWithAllowedClass_then_success() {
        String name = "map";
        javaMapping(name, Integer.class, AllowedClass.class).create();
        instance().getMap(name).put(1, "initial");
        assertThatNoException().isThrownBy(() -> execute("UPDATE map SET allowed = 100 WHERE __key = 1"));
        assertThatNoException().isThrownBy(() -> execute("UPDATE map SET allowed = null WHERE __key = 1"));
    }

    private static SqlMapping javaMapping(String name, Class<?> keyClass, Class<?> valueClass) {
        return new SqlMapping(name, IMapSqlConnector.class)
            .options(
                OPTION_KEY_FORMAT, JAVA_FORMAT,
                OPTION_KEY_CLASS, keyClass.getName(),
                OPTION_VALUE_FORMAT, JAVA_FORMAT,
                OPTION_VALUE_CLASS, valueClass.getName()
            );
    }

    private void execute(String sql) {
        try (var ignore = sqlService.execute(sql)) { }
    }

    public static class AllowedClass implements Serializable {
        private Integer allowed;

        public Integer getAllowed() {
            return allowed;
        }

        public void setAllowed(Integer value) {
            this.allowed = value;
        }

    }

    public static class NotAllowedClass implements Serializable {
        private Integer restricted;

        public Integer getRestricted() {
            return restricted;
        }

        public void setRestricted(Integer value) {
            this.restricted = value;
        }

    }
}
