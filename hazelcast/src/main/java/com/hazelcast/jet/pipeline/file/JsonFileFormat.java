/*
 * Copyright (c) 2008-2022, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.jet.pipeline.file;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.Objects;

/**
 * {@code FileFormat} for the JSON Lines files. See {@link FileFormat#json}
 * for more details.
 *
 * @param <T> type of items a source using this file format will emit
 * @since Jet 4.4
 */
public class JsonFileFormat<T> implements FileFormat<T> {

    /**
     * Format ID for JSON.
     */
    public static final String FORMAT_JSON = "json";

    private static final long serialVersionUID = 1L;

    private Class<T> clazz;
    private boolean multiline = true;

    /**
     * Creates {@link JsonFileFormat}. See {@link FileFormat#json} for more
     * details.
     */
    JsonFileFormat() {
    }

    /**
     * Specifies class that data will be deserialized into.
     * If parameter is {@code null} data is deserialized into
     * {@code Map<String, Object>}.
     *
     * @param clazz type of the object to deserialize JSON into
     */
    @Nonnull
    public JsonFileFormat<T> withClass(@Nullable Class<T> clazz) {
        this.clazz = clazz;
        return this;
    }

    /**
     * Specifies if the Json parser should accept json records spanning
     * multiple lines.
     * <p>
     * The parser handles JSON records spanning multiple lines by default,
     * but it prevents reading the file in parallel when using the Hadoop
     * based connector, because the file is split at arbitrary positions.
     * <p>
     * Set this to false when reading large JSON files using Hadoop
     * connector. Each line in the file must contain exactly one JSON record.
     * <p>
     * This setting has no effect when Hadoop is not used.
     *
     * @param multiline true, if the JSON parser should accept records
     *                  spanning multiple lines, defaults to true
     */
    @Nonnull
    public JsonFileFormat<T> multiline(boolean multiline) {
        this.multiline = multiline;
        return this;
    }

    /**
     * Returns the class Jet will deserialize data into.
     * Null if not set.
     */
    @Nullable
    public Class<T> clazz() {
        return clazz;
    }

    /**
     * Specifies if the Json parser should accept json records spanning
     * multiple lines.
     */
    public boolean isMultiline() {
        return multiline;
    }

    @Nonnull
    @Override
    public String format() {
        return FORMAT_JSON;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        JsonFileFormat<?> that = (JsonFileFormat<?>) o;
        return multiline == that.multiline && Objects.equals(clazz, that.clazz);
    }

    @Override
    public int hashCode() {
        return Objects.hash(clazz, multiline);
    }
}
