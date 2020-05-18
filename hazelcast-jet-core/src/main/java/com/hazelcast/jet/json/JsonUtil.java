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
import com.hazelcast.function.FunctionEx;
import com.hazelcast.jet.pipeline.FileSourceBuilder;
import com.hazelcast.jet.pipeline.Sources;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.Reader;
import java.nio.file.Path;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Spliterator;
import java.util.Spliterators;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import static java.nio.charset.StandardCharsets.UTF_8;

/**
 * Util class to parse JSON formatted input to various object types or
 * convert objects to JSON strings.
 * <p>
 * We use the lightweight JSON library `jackson-jr` to parse the given
 * input or to convert the given objects to JSON string. If
 * `jackson-annotations` library present on the classpath, we register
 * {@link JacksonAnnotationExtension} to so that the JSON conversion can
 * make use of <a href="https://github.com/FasterXML/jackson-annotations/wiki/Jackson-Annotations">
 * Jackson Annotations</a>.
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
    public static HazelcastJsonValue hazelcastJsonValue(@Nonnull Object object) {
        return new HazelcastJsonValue(object.toString());
    }

    /**
     * Converts a JSON string to a object of given type.
     */
    @Nullable
    public static <T> T beanFrom(@Nonnull Class<T> type, @Nonnull String jsonString) throws IOException {
        return JSON_JR.beanFrom(type, jsonString);
    }

    /**
     * Converts a JSON string to a {@link Map}.
     */
    @Nullable
    public static Map<String, Object> mapFrom(@Nonnull String jsonString) throws IOException {
        return JSON_JR.mapFrom(jsonString);
    }

    /**
     * Converts a JSON string to a {@link List} of given type.
     */
    @Nullable
    public static <T> List<T> listFrom(@Nonnull Class<T> type, @Nonnull String jsonString) throws IOException {
        return JSON_JR.listOfFrom(type, jsonString);
    }

    /**
     * Converts a JSON string to a {@link List}.
     */
    @Nullable
    public static List<Object> listFrom(@Nonnull String jsonString) throws IOException {
        return JSON_JR.listFrom(jsonString);
    }

    /**
     * Converts a JSON string to an Object. The returned object will differ
     * according to the content of the string:
     * <ul>
     *     <li>content is a JSON object, returns a {@link Map}. See
     *     {@link #mapFrom(String)}.</li>
     *     <li>content is a JSON array, returns a {@link List}. See
     *     {@link #listFrom(String)}.</li>
     *     <li>content is a String, null or primitive, returns String, null or
     *     primitive.</li>
     * </ul>
     */
    @Nullable
    public static Object anyFrom(@Nonnull String jsonString) throws IOException {
        return JSON_JR.anyFrom(jsonString);
    }

    /**
     * Returns an {@link Iterator} over the sequence of JSON objects parsed
     * from given JSON string.
     */
    @Nonnull
    public static <T> Iterator<T> sequenceFrom(@Nonnull Class<T> type, @Nonnull String jsonString)
            throws IOException {
        return JSON_JR.beanSequenceFrom(type, jsonString);
    }

    /**
     * Returns an {@link Iterator} over the sequence of JSON objects parsed
     * from given {@code reader}.
     */
    @Nonnull
    public static <T> Iterator<T> sequenceFrom(@Nonnull Class<T> type, @Nonnull Reader reader)
            throws IOException {
        return JSON_JR.beanSequenceFrom(type, reader);
    }

    /**
     * Returns a function which takes a file {@code Path} as input and
     * returns a stream of objects with the given type. The content of the file
     * is considered to have a sequence of JSON strings, each one can span
     * multiple lines. The function is designed to be used with
     * {@link FileSourceBuilder#build(FunctionEx)}.
     * <p>
     * See {@link Sources#json(String, Class)}.
     */
    @Nonnull
    public static <T> FunctionEx<? super Path, ? extends Stream<T>> asMultilineJson(
            @Nonnull Class<T> type
    ) {
        return path -> {
            InputStreamReader reader = new InputStreamReader(new FileInputStream(path.toFile()), UTF_8);
            Spliterator<T> spliterator = Spliterators.spliteratorUnknownSize(JsonUtil.sequenceFrom(type, reader),
                    Spliterator.ORDERED | Spliterator.NONNULL);
            return StreamSupport.stream(spliterator, false);
        };
    }

    /**
     * Creates a JSON string for the given object.
     */
    @Nonnull
    public static String toJson(@Nonnull Object object) throws IOException {
        return JSON_JR.asString(object);
    }

}
