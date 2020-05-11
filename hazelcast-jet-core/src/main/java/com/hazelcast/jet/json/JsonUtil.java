/*
 * Copyright (c) 2008-2020, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.jet.json;

import com.fasterxml.jackson.jr.annotationsupport.JacksonAnnotationExtension;
import com.fasterxml.jackson.jr.ob.JSON;
import com.hazelcast.core.HazelcastJsonValue;

import javax.annotation.Nonnull;
import java.io.Reader;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import static com.hazelcast.jet.impl.util.Util.uncheckCall;

/**
 * Util class to parse JSON formatted input to various object types or
 * convert objects to JSON strings.
 * <p>
 * We use the lightweight JSON library `jackson-jr` to parse the given
 * input or convert the given objects to JSON string. If
 * `jackson-annotations` library present on the classpath, we register
 * {@link JacksonAnnotationExtension} to so that the JSON conversion can
 * make us of annotations.
 *
 * @since 4.2
 */
public final class JsonUtil {

    private static final JSON JSON_JR;

    static {
        JSON.Builder builder = JSON.builder();
        try {
            Class.forName("com.fasterxml.jackson.annotation.JacksonAnnotation", false, JsonUtil.class.getClassLoader());
            builder.register(JacksonAnnotationExtension.std);
        } catch (ClassNotFoundException ignored) {
        }
        JSON_JR = builder.build();
    }

    private JsonUtil() {
    }

    /**
     * Creates a {@link HazelcastJsonValue} by converting given the object to
     * string using {@link Object#toString()}.
     */
    @Nonnull
    public static <T> HazelcastJsonValue hazelcastJsonValue(@Nonnull T object) {
        return new HazelcastJsonValue(object.toString());
    }

    /**
     * Creates a {@link HazelcastJsonValue} by converting the key of the given
     * entry to string using {@link Object#toString()}.
     */
    @Nonnull
    public static <K> HazelcastJsonValue asJsonKey(@Nonnull Map.Entry<K, ?> entry) {
        return new HazelcastJsonValue(entry.getKey().toString());
    }

    /**
     * Creates a {@link HazelcastJsonValue} by converting the value of the
     * given entry to string using {@link Object#toString()}.
     */
    @Nonnull
    public static <V> HazelcastJsonValue asJsonValue(@Nonnull Map.Entry<?, V> entry) {
        return new HazelcastJsonValue(entry.getValue().toString());
    }

    /**
     * Converts a JSON string to a object of given type.
     */
    @Nonnull
    public static <T> T parse(@Nonnull Class<T> type, @Nonnull String jsonString) {
        return uncheckCall(() -> JSON_JR.beanFrom(type, jsonString));
    }

    /**
     * Converts the contents of the specified {@code reader} to a object of
     * given type.
     */
    @Nonnull
    public static <T> T parse(@Nonnull Class<T> type, @Nonnull Reader reader) {
        return uncheckCall(() -> JSON_JR.beanFrom(type, reader));
    }

    /**
     * Converts a JSON string to a {@link Map}.
     */
    @Nonnull
    public static Map<String, Object> parse(@Nonnull String jsonString) {
        return uncheckCall(() -> JSON_JR.mapFrom(jsonString));
    }

    /**
     * Converts the contents of the specified {@code reader} to a {@link Map}.
     */
    @Nonnull
    public static Map<String, Object> parse(@Nonnull Reader reader) {
        return uncheckCall(() -> JSON_JR.mapFrom(reader));
    }

    /**
     * Returns an {@link Iterator} over the sequence of JSON objects parsed
     * from given {@code reader}.
     */
    @Nonnull
    public static <T> Iterator<T> parseSequence(@Nonnull Class<T> type, @Nonnull Reader reader) {
        return uncheckCall(() -> JSON_JR.beanSequenceFrom(type, reader));
    }

    /**
     * Extracts a string value from given JSON string. For extracting multiple
     * values from a JSON string see {@link #parse(String)}.
     */
    @Nonnull
    public static String getString(@Nonnull String jsonString, @Nonnull String key) {
        return (String) parse(jsonString).get(key);
    }

    /**
     * Extracts an integer value from given JSON string. For extracting
     * multiple values from a JSON string see {@link #parse(String)}.
     */
    public static int getInt(@Nonnull String jsonString, @Nonnull String key) {
        return (int) parse(jsonString).get(key);
    }

    /**
     * Extracts a boolean value from given JSON string. For extracting
     * multiple values from a JSON string see {@link #parse(String)}.
     */
    public static boolean getBoolean(@Nonnull String jsonString, @Nonnull String key) {
        return (boolean) parse(jsonString).get(key);
    }

    /**
     * Extracts an array value as a {@link List} from given JSON string. For
     * extracting multiple values from a JSON string see {@link #parse(String)}.
     */
    @Nonnull
    public static List<Object> getList(@Nonnull String jsonString, @Nonnull String key) {
        return (List) parse(jsonString).get(key);
    }

    /**
     * Extracts an array value as a {@link Object Object[]} from given JSON
     * string. For extracting multiple values from a JSON string see
     * {@link #parse(String)}.
     */
    @Nonnull
    public static Object[] getArray(@Nonnull String jsonString, @Nonnull String key) {
        return getList(jsonString, key).toArray();
    }

    /**
     * Extracts an object as a {@link Map} from given JSON string. For
     * extracting multiple values from a JSON string see {@link #parse(String)}.
     */
    @Nonnull
    public static Map<String, Object> getObject(@Nonnull String jsonString, @Nonnull String key) {
        return (Map<String, Object>) parse(jsonString).get(key);
    }

    /**
     * Creates a JSON string for the given object.
     */
    @Nonnull
    public static <T> String asString(@Nonnull T object) {
        return uncheckCall(() -> JSON_JR.asString(object));
    }

}
