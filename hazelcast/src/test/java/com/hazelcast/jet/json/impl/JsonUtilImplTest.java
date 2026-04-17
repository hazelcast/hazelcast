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
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.junitpioneer.jupiter.ClearSystemProperty;
import org.junitpioneer.jupiter.SetSystemProperty;

import java.util.function.BiFunction;
import java.util.function.Function;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

@ClearSystemProperty(key = JsonUtilImpl.JSON_BLOCKLIST_DEFAULTS_DISABLED_PROPERTY)
public class JsonUtilImplTest {

    static final String JSON = "{}";

    enum FunctionUnderTest implements Function<Class<?>, Object> {

        PublicToBean {
            @Override
            public Object apply(Class<?> type) {
                return JsonUtil.toBean(type).apply(JSON);
            }
        },

        BeanFromFn {
            @Override
            public Object apply(Class<?> type) {
                return JsonUtilImpl.toBean(type).apply(JSON);
            }
        },

        BeanFromBiFn {
            @Override
            public Object apply(Class<?> type) {
                return JsonUtilImpl.toBeanBiFn(type).apply(null, JSON);
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

    @ParameterizedTest
    @EnumSource
    void shouldCreate_whenAllowedClass(FunctionUnderTest functionUnderTest) {
        assertThat(functionUnderTest.apply(MyDto.class)).isInstanceOf(MyDto.class);
    }

    @ParameterizedTest
    @EnumSource
    @SetSystemProperty(key = JsonUtilImpl.JSON_BLOCKLIST_DEFAULTS_DISABLED_PROPERTY, value = "true")
    void shouldCreate_whenAllowedClassAndDefaultsDisabled(FunctionUnderTest functionUnderTest) {
        assertThat(functionUnderTest.apply(MyDto.class)).isInstanceOf(MyDto.class);
    }

    @ParameterizedTest
    @EnumSource
    void shouldNotCreate_whenBlocklistedClass(FunctionUnderTest functionUnderTest) {
        assertThatThrownBy(() -> functionUnderTest.apply(MapConfig.class))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("cannot be deserialized using JSON");
    }

    @ParameterizedTest
    @EnumSource
    @SetSystemProperty(key = JsonUtilImpl.JSON_BLOCKLIST_DEFAULTS_DISABLED_PROPERTY, value = "true")
    void shouldCreate_whenBlocklistedClassAndDefaultsDisabled(FunctionUnderTest functionUnderTest) {
        assertThat(functionUnderTest.apply(MapConfig.class)).isInstanceOf(MapConfig.class);
    }

    @ParameterizedTest
    @EnumSource
    void shouldDeserialize_whenAllowedClass(FunctionForSerialization functionUnderTest) {
        // given
        var function = functionUnderTest.apply(MyDto.class);
        var ss = new DefaultSerializationServiceBuilder().build();
        var bytes = ss.toData(function);

        // when
        var deserialized = ss.toObject(bytes);

        // then
        assertThatDeserializedFunctionCreates(deserialized, MyDto.class);
    }

    @ParameterizedTest
    @EnumSource
    @SetSystemProperty(key = JsonUtilImpl.JSON_BLOCKLIST_DEFAULTS_DISABLED_PROPERTY, value = "true")
    void shouldDeserialize_whenAllowedClassAndDefaultsDisabled(FunctionForSerialization functionUnderTest) {
        // given
        var function = functionUnderTest.apply(MyDto.class);
        var ss = new DefaultSerializationServiceBuilder().build();
        var bytes = ss.toData(function);

        // when
        var deserialized = ss.toObject(bytes);

        // then
        assertThatDeserializedFunctionCreates(deserialized, MyDto.class);
    }

    @ParameterizedTest
    @EnumSource
    @SetSystemProperty(key = JsonUtilImpl.JSON_BLOCKLIST_DEFAULTS_DISABLED_PROPERTY, value = "true")
    void shouldDeserialize_whenBlocklistedClassAndDefaultsDisabled(FunctionForSerialization functionUnderTest) {
        // given
        var function = functionUnderTest.apply(MapConfig.class);
        var ss = new DefaultSerializationServiceBuilder().build();
        var bytes = ss.toData(function);

        // when
        var deserialized = ss.toObject(bytes);

        // then
        assertThatDeserializedFunctionCreates(deserialized, MapConfig.class);
    }

    @ParameterizedTest
    @EnumSource
    @SetSystemProperty(key = JsonUtilImpl.JSON_BLOCKLIST_DEFAULTS_DISABLED_PROPERTY, value = "true")
    void shouldNotDeserializeFunction_whenBlocklistedClass(FunctionForSerialization functionUnderTest) {
        var function = functionUnderTest.apply(MapConfig.class);
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
            assertThat(fun.apply(JSON)).isInstanceOf(expectedType);
        } else {
            assertThat(((BiFunction) deserialized).apply(null, JSON)).isInstanceOf(expectedType);
        }
    }

    public static class MyDto {
        public MyDto() {
        }
    }
}
