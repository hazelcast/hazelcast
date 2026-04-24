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
import com.hazelcast.internal.serialization.impl.DefaultSerializationServiceBuilder;
import com.hazelcast.jet.json.JsonUtil;
import org.junitpioneer.jupiter.ClearSystemProperty;
import org.junitpioneer.jupiter.SetSystemProperty;
import org.junitpioneer.jupiter.cartesian.CartesianTest;

import java.lang.annotation.Documented;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;
import java.util.function.BiFunction;
import java.util.function.Function;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

@ClearSystemProperty(key = JsonUtilImpl.JSON_BLOCKLIST_DEFAULTS_DISABLED_PROPERTY)
public class JsonUtilImplTest {

    static final String JSON = "{}";

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

    @CartesianTest
    void shouldCreate_whenAllowedClass(@CartesianTest.Enum FunctionUnderTest functionUnderTest,
                                       @ClassesAllowedByDefault Class<?> clazz) {
        assertThat(functionUnderTest.apply(clazz, wrapJsonInArrayIfNeeded(JSON, clazz))).isInstanceOf(clazz);
    }

    @CartesianTest
    @SetSystemProperty(key = JsonUtilImpl.JSON_BLOCKLIST_DEFAULTS_DISABLED_PROPERTY, value = "true")
    void shouldCreate_whenAllowedClassAndDefaultsDisabled(@CartesianTest.Enum FunctionUnderTest functionUnderTest,
                                                          @ClassesAllowedByDefault Class<?> clazz) {
        assertThat(functionUnderTest.apply(clazz, wrapJsonInArrayIfNeeded(JSON, clazz))).isInstanceOf(clazz);
    }

    @CartesianTest
    void shouldNotCreate_whenBlocklistedClass(@CartesianTest.Enum FunctionUnderTest functionUnderTest,
                                              @ClassesBlocklistedByDefault Class<?> clazz) {
        assertThatThrownBy(() -> functionUnderTest.apply(clazz, wrapJsonInArrayIfNeeded(JSON, clazz)))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("cannot be deserialized using JSON");
    }

    @CartesianTest
    @SetSystemProperty(key = JsonUtilImpl.JSON_BLOCKLIST_DEFAULTS_DISABLED_PROPERTY, value = "true")
    void shouldCreate_whenBlocklistedClassAndDefaultsDisabled(@CartesianTest.Enum FunctionUnderTest functionUnderTest,
                                                              @ClassesBlocklistedByDefault Class<?> clazz) {
        assertThat(functionUnderTest.apply(clazz, wrapJsonInArrayIfNeeded(JSON, clazz))).isInstanceOf(clazz);
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

        // when
        var deserialized = ss.toObject(bytes);

        // then
        assertThatDeserializedFunctionCreates(deserialized, clazz);
    }

    @CartesianTest
    @SetSystemProperty(key = JsonUtilImpl.JSON_BLOCKLIST_DEFAULTS_DISABLED_PROPERTY, value = "true")
    void shouldNotDeserializeFunction_whenBlocklistedClass(@CartesianTest.Enum FunctionForSerialization functionUnderTest,
                                                           @ClassesBlocklistedByDefault Class<?> clazz) {
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
            assertThat(fun.apply(wrapJsonInArrayIfNeeded(JSON, expectedType)))
                    .isInstanceOf(expectedType);
        } else {
            assertThat(((BiFunction) deserialized).apply(null, wrapJsonInArrayIfNeeded(JSON, expectedType)))
                    .isInstanceOf(expectedType);
        }
    }

    public static class MyDto {
        public MyDto() {
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
    @CartesianTest.Values(classes = {MapConfig.class, MapConfig[].class, MapConfig[][].class, MapConfig[][][].class})
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
}

