/*
 * Copyright (c) 2008-2026, Hazelcast, Inc. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.hazelcast.jet.json.impl;

import com.hazelcast.config.MapConfig;
import com.hazelcast.internal.json.Json;
import com.hazelcast.internal.serialization.impl.DefaultSerializationServiceBuilder;
import com.hazelcast.jet.impl.util.JsonUtilTest;
import com.hazelcast.jet.json.JsonUtil;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Nested;
import org.junitpioneer.jupiter.ClearSystemProperty;
import org.junitpioneer.jupiter.SetSystemProperty;
import org.junitpioneer.jupiter.cartesian.CartesianTest;
import tools.jackson.core.json.JsonFactory;
import tools.jackson.core.json.JsonReadFeature;
import tools.jackson.jr.annotationsupport.JacksonAnnotationExtension;
import tools.jackson.jr.ob.JSON;

import javax.sql.DataSource;
import java.io.IOException;
import java.lang.annotation.Documented;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.function.BiFunction;
import java.util.function.Function;

import static com.hazelcast.jet.json.impl.JsonUtilImpl.getElementType;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatNoException;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assumptions.assumeThat;

/**
 * Note: {@link JsonUtil} has a static {@link JSON} instance with {@link JacksonJrFilteringExtension}
 * that checks env variables early and only once. Updates to environment are not reflected
 * in the {@link JsonUtil#toJson(Object)}/{@link JsonUtil#beanFrom(String, Class)} behavior.
 */
@ClearSystemProperty(key = JsonUtilImpl.JSON_BLOCKLIST_DEFAULTS_DISABLED_PROPERTY)
public class JsonUtilImplTest {

    static final String TEST_JSON = Json.object()
            .add("backupCount", "2")
            // used by MyDtoWithBlockedField, ignored by other classes
            .add("config", Json.object()
                    .add("backupCount", "2"))
            .toString();

    enum FunctionUnderTest implements BiFunction<Class<?>, String, Object> {

        PublicToBean {
            @Override
            public Object apply(Class<?> type, String json) {
                return JsonUtil.toBean(type).apply(json);
            }
        },

        BeanFromFn {
            @Override
            public Object apply(Class<?> type, String json) {
                return JsonUtilImpl.toBean(type).apply(json);
            }
        },

        BeanFromBiFn {
            @Override
            public Object apply(Class<?> type, String json) {
                return JsonUtilImpl.toBeanBiFn(type).apply(null, json);
            }
        }
    }

    enum FunctionForSerialization implements Function<Class<?>, Object> {

        PublicToBean {
            @Override
            public Object apply(Class<?> type) {
                return JsonUtil.toBean(type);
            }
        },

        BeanFromFn {
            @Override
            public Object apply(Class<?> type) {
                return JsonUtilImpl.toBean(type);
            }
        },

        BeanFromBiFn {
            @Override
            public Object apply(Class<?> type) {
                return JsonUtilImpl.toBeanBiFn(type);
            }
        }
    }

    @BeforeAll
    static void initializeJsonUtilWithDefaultEnvironment() throws IOException {
        // Ensures that JsonUtil is initialed first time without the defaults disabled property
        JsonUtil.toJson(new Object());
    }

    @CartesianTest
    void shouldCreate_whenAllowedClass(@CartesianTest.Enum FunctionUnderTest functionUnderTest,
                                       @ClassesAllowedByDefault Class<?> clazz) {
        assertThat(functionUnderTest.apply(clazz, wrapJsonInArrayIfNeeded(TEST_JSON, clazz))).isInstanceOf(clazz);
    }

    @CartesianTest
    @SetSystemProperty(key = JsonUtilImpl.JSON_BLOCKLIST_DEFAULTS_DISABLED_PROPERTY, value = "true")
    void shouldCreate_whenAllowedClassAndDefaultsDisabled(@CartesianTest.Enum FunctionUnderTest functionUnderTest,
                                                          @ClassesAllowedByDefault Class<?> clazz) {
        assertThat(functionUnderTest.apply(clazz, wrapJsonInArrayIfNeeded(TEST_JSON, clazz))).isInstanceOf(clazz);
    }

    @CartesianTest
    void shouldNotCreate_whenBlocklistedClass(@CartesianTest.Enum FunctionForSerialization functionUnderTest,
                                              @ClassesBlocklistedByDefault Class<?> clazz) {
        assumeThat(getElementType(clazz))
                .as("High-level functions do not check fields")
                .isNotEqualTo(MyDtoWithBlockedField.class);

        assertThatThrownBy(() -> functionUnderTest.apply(clazz))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("cannot be deserialized using JSON");
    }

    @CartesianTest
    @SetSystemProperty(key = JsonUtilImpl.JSON_BLOCKLIST_DEFAULTS_DISABLED_PROPERTY, value = "true")
    void shouldCreate_whenBlocklistedClassAndDefaultsDisabled(@CartesianTest.Enum FunctionForSerialization functionUnderTest,
                                                              @ClassesBlocklistedByDefault Class<?> clazz) {
        assertThatNoException().isThrownBy(() -> functionUnderTest.apply(clazz));
    }

    @CartesianTest
    void shouldCreate_allowedClassAfterBlocklisted(@CartesianTest.Enum FunctionUnderTest functionUnderTest,
                                                   @ClassesAllowedByDefault Class<?> clazz) {
        // ensure that the blocking does not interfere
        assertThatThrownBy(() -> functionUnderTest.apply(MapConfig.class, wrapJsonInArrayIfNeeded(TEST_JSON, clazz)))
                .isInstanceOf(IllegalArgumentException.class);
        assertThat(functionUnderTest.apply(clazz, wrapJsonInArrayIfNeeded(TEST_JSON, clazz))).isInstanceOf(clazz);
    }

    @CartesianTest
    void shouldDeserialize_whenAllowedClass(@CartesianTest.Enum FunctionForSerialization functionUnderTest,
                                            @ClassesAllowedByDefault Class<?> clazz) {
        // given
        var function = functionUnderTest.apply(clazz);
        var ss = new DefaultSerializationServiceBuilder().build();
        var bytes = ss.toData(function);

        // when
        var deserialized = ss.toObject(bytes);

        // then
        assertThatDeserializedFunctionCreates(deserialized, clazz);
    }

    @CartesianTest
    @SetSystemProperty(key = JsonUtilImpl.JSON_BLOCKLIST_DEFAULTS_DISABLED_PROPERTY, value = "true")
    void shouldDeserialize_whenAllowedClassAndDefaultsDisabled(@CartesianTest.Enum FunctionForSerialization functionUnderTest,
                                                               @ClassesAllowedByDefault Class<?> clazz) {
        // given
        var function = functionUnderTest.apply(clazz);
        var ss = new DefaultSerializationServiceBuilder().build();
        var bytes = ss.toData(function);

        // when
        var deserialized = ss.toObject(bytes);

        // then
        assertThatDeserializedFunctionCreates(deserialized, clazz);
    }

    @CartesianTest
    @SetSystemProperty(key = JsonUtilImpl.JSON_BLOCKLIST_DEFAULTS_DISABLED_PROPERTY, value = "true")
    void shouldDeserialize_whenBlocklistedClassAndDefaultsDisabled(@CartesianTest.Enum FunctionForSerialization functionUnderTest,
                                                                   @ClassesBlocklistedByDefault Class<?> clazz) {
        // given
        var function = functionUnderTest.apply(clazz);
        var ss = new DefaultSerializationServiceBuilder().build();
        var bytes = ss.toData(function);

        assertThatNoException().isThrownBy(() -> ss.toObject(bytes));
    }

    @CartesianTest
    @SetSystemProperty(key = JsonUtilImpl.JSON_BLOCKLIST_DEFAULTS_DISABLED_PROPERTY, value = "true")
    void shouldNotDeserializeFunction_whenBlocklistedClass(@CartesianTest.Enum FunctionForSerialization functionUnderTest,
                                                           @ClassesBlocklistedByDefault Class<?> clazz) {
        assumeThat(getElementType(clazz))
                .as("High-level functions do not check fields")
                .isNotEqualTo(MyDtoWithBlockedField.class);

        var function = functionUnderTest.apply(clazz);
        var ss = new DefaultSerializationServiceBuilder().build();
        var bytes = ss.toData(function);

        System.clearProperty(JsonUtilImpl.JSON_BLOCKLIST_DEFAULTS_DISABLED_PROPERTY);

        assertThatThrownBy(() -> ss.toObject(bytes))
                .rootCause()
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("cannot be deserialized using JSON");
    }

    @SuppressWarnings({"rawtypes", "unchecked"})
    private void assertThatDeserializedFunctionCreates(Object deserialized, Class<?> expectedType) {
        if (deserialized instanceof Function fun) {
            assertThat(fun.apply(wrapJsonInArrayIfNeeded(TEST_JSON, expectedType)))
                    .isInstanceOf(expectedType);
        } else {
            assertThat(((BiFunction) deserialized).apply(null, wrapJsonInArrayIfNeeded(TEST_JSON, expectedType)))
                    .isInstanceOf(expectedType);
        }
    }

    public static class MyDto {
        private int backupCount;

        public MyDto() {
        }

        public MyDto(int backupCount) {
            this.backupCount = backupCount;
        }

        public int getBackupCount() {
            return backupCount;
        }

        public void setBackupCount(int backupCount) {
            this.backupCount = backupCount;
        }

        @Override
        public String toString() {
            return "MyDto{" +
                    "backupCount=" + backupCount +
                    '}';
        }

        @Override
        public boolean equals(Object o) {
            if (!(o instanceof MyDto myDto)) {
                return false;
            }
            return backupCount == myDto.backupCount;
        }

        @Override
        public int hashCode() {
            return Objects.hashCode(backupCount);
        }
    }

    public static class MyDtoWithBlockedField {
        private MapConfig config;

        public MyDtoWithBlockedField() {
        }

        public MyDtoWithBlockedField(MapConfig config) {
            this.config = config;
        }

        public MapConfig getConfig() {
            return config;
        }

        public void setConfig(MapConfig config) {
            this.config = config;
        }

        @Override
        public String toString() {
            return "MyDtoWithBlockedField{" +
                    "config=" + config +
                    '}';
        }
    }

    @Target({ElementType.PARAMETER, ElementType.ANNOTATION_TYPE})
    @Retention(RetentionPolicy.RUNTIME)
    @Documented
    @CartesianTest.Values(classes = {MyDto.class, MyDto[].class, MyDto[][].class, MyDto[][][].class})
    @interface ClassesAllowedByDefault {
    }

    @Target({ElementType.PARAMETER, ElementType.ANNOTATION_TYPE})
    @Retention(RetentionPolicy.RUNTIME)
    @Documented
    @CartesianTest.Values(classes = {MapConfig.class, MapConfig[].class, MapConfig[][].class, MapConfig[][][].class,
            MyDtoWithBlockedField.class, MyDtoWithBlockedField[].class,
            JsonUtilTest.DummyDataSource.class})
    @interface ClassesBlocklistedByDefault {
    }

    private static String wrapJsonInArrayIfNeeded(String json, Class<?> type) {
        int levels = 0;
        while (type.isArray()) {
            levels++;
            type = type.getComponentType();
        }
        return "[".repeat(levels) + json + "]".repeat(levels);
    }

    /**
     * Low-level tests of JacksonJrFilteringExtension with JSON, but without JsonUtil
     */
    @Nested
    class Extension {

        enum ValueWrapper implements Function<Object, Object> {
            OBJECT {
                @Override
                public Object apply(Object value) {
                    return value;
                }
            },
            LIST {
                @Override
                public Object apply(Object o) {
                    return List.of("aa", o, 123);
                }
            },
            ARRAY {
                @Override
                public Object apply(Object o) {
                    return new Object[]{o};
                }
            },
            ARRAY2D {
                @Override
                public Object apply(Object o) {
                    return new Object[][]{{123}, {o, "aa"}, {o, o}};
                }
            }
        }

        @BeforeAll
        static void initializeJsonUtilWithDefaultEnvironment() throws IOException {
            // Ensures that JsonUtil is initialed first time without the defaults disabled property
            JsonUtil.toJson(new Object());
        }

        @CartesianTest
        void shouldCreate_whenAllowedClass(@ClassesAllowedByDefault Class<?> clazz,
                                           @CartesianTest.Values(booleans = {true, false}) boolean jacksonAnnotationPresent) {
            assertThatAllowedDeserializedCorrectly(clazz, jacksonAnnotationPresent);
        }

        @CartesianTest
        @SetSystemProperty(key = JsonUtilImpl.JSON_BLOCKLIST_DEFAULTS_DISABLED_PROPERTY, value = "true")
        void shouldCreate_whenAllowedClassAndDefaultsDisabled(@ClassesAllowedByDefault Class<?> clazz,
                                                              @CartesianTest.Values(booleans = {true, false}) boolean jacksonAnnotationPresent) {
            assertThatAllowedDeserializedCorrectly(clazz, jacksonAnnotationPresent);
        }


        private void assertThatAllowedDeserializedCorrectly(Class<?> clazz, boolean jacksonAnnotationPresent) {
            var obj = jsonJr(jacksonAnnotationPresent)
                    .beanFrom(clazz, wrapJsonInArrayIfNeeded(TEST_JSON, clazz));

            if (clazz.isArray()) {
                assertThat(Arrays.deepToString((Object[]) obj)).contains("backupCount=2");
            } else {
                assertThat(obj).isEqualTo(new MyDto(2));
            }
        }

        @CartesianTest
        void shouldNotCreate_whenBlocklistedClass(@ClassesBlocklistedByDefault Class<?> clazz,
                                                  @CartesianTest.Values(booleans = {true, false}) boolean jacksonAnnotationPresent) {
            assertThatThrownBy(() -> jsonJr(jacksonAnnotationPresent)
                    .beanFrom(clazz, wrapJsonInArrayIfNeeded(TEST_JSON, clazz)))
                    .hasMessageContaining("cannot be deserialized using JSON");
        }

        @CartesianTest
        @SetSystemProperty(key = JsonUtilImpl.JSON_BLOCKLIST_DEFAULTS_DISABLED_PROPERTY, value = "true")
        void shouldCreate_whenBlocklistedClassAndDefaultsDisabled(@ClassesBlocklistedByDefault Class<?> clazz,
                                                                  @CartesianTest.Values(booleans = {true, false}) boolean jacksonAnnotationPresent) {
            var obj = jsonJr(jacksonAnnotationPresent)
                    .beanFrom(clazz, wrapJsonInArrayIfNeeded(TEST_JSON, clazz));

            if (clazz.isArray()) {
                assertThat(Arrays.deepToString((Object[]) obj)).contains("backupCount=2");
            } else {
                if (DataSource.class.isAssignableFrom(clazz)) {
                    // special case in test setup
                    assertThat(obj).isInstanceOf(JsonUtilTest.DummyDataSource.class);
                } else {
                    assertThat(obj).asString().contains("backupCount=2");
                }
            }
        }

        @CartesianTest
        void shouldWrite_whenAllowedClass(@CartesianTest.Enum ValueWrapper valueWrapper,
                                          @CartesianTest.Values(booleans = {true, false}) boolean jacksonAnnotationPresent) {
            assertThat(jsonJr(jacksonAnnotationPresent)
                    .asString(valueWrapper.apply(new MyDto(2)))).contains("\"backupCount\":2");
        }

        @CartesianTest
        @SetSystemProperty(key = JsonUtilImpl.JSON_BLOCKLIST_DEFAULTS_DISABLED_PROPERTY, value = "true")
        void shouldWrite_whenAllowedClassAndDefaultsDisabled(@CartesianTest.Enum ValueWrapper valueWrapper,
                                                             @CartesianTest.Values(booleans = {true, false}) boolean jacksonAnnotationPresent) {
            assertThat(jsonJr(jacksonAnnotationPresent)
                    .asString(valueWrapper.apply(new MyDto(2)))).contains("\"backupCount\":2");
        }

        @CartesianTest
        void shouldNotWrite_whenBlocklistedClass(@CartesianTest.Enum ValueWrapper valueWrapper,
                                                 @CartesianTest.Values(booleans = {true, false}) boolean jacksonAnnotationPresent) {
            assertThatThrownBy(() -> jsonJr(jacksonAnnotationPresent)
                    .asString(valueWrapper.apply(new MapConfig().setBackupCount(2))))
                    .hasMessageContaining("cannot be serialized using JSON");
        }

        @CartesianTest
        @SetSystemProperty(key = JsonUtilImpl.JSON_BLOCKLIST_DEFAULTS_DISABLED_PROPERTY, value = "true")
        void shouldWrite_whenBlocklistedClassAndDefaultsDisabled(@CartesianTest.Enum ValueWrapper valueWrapper,
                                                                 @CartesianTest.Values(booleans = {true, false}) boolean jacksonAnnotationPresent) {
            assertThat(jsonJr(jacksonAnnotationPresent)
                    .asString(valueWrapper.apply(new MapConfig().setBackupCount(2)))).contains("\"backupCount\":2");
        }

        /**
         * @return {@link JSON} with similar configuration to {@link JsonUtil#JSON_JR}
         */
        private static JSON jsonJr(boolean jacksonAnnotationPresent) {
            JsonFactory jf = JsonFactory.builder()
                    .enable(JsonReadFeature.ALLOW_BACKSLASH_ESCAPING_ANY_CHARACTER)
                    .enable(JsonReadFeature.ALLOW_UNESCAPED_CONTROL_CHARS)
                    .build();
            JSON.Builder builder = JSON.builder(jf)
                    .register(new JacksonJrFilteringExtension())
                    .enable(JSON.Feature.ACCEPT_CASE_INSENSITIVE_PROPERTIES);
            if (jacksonAnnotationPresent) {
                builder.register(JacksonAnnotationExtension.std);
            }
            return builder.build();
        }
    }
}
