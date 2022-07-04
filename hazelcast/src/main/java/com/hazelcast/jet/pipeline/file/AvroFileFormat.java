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
 * {@link FileFormat} for avro files. See {@link FileFormat#avro} for more
 * details.
 *
 * @param <T> type of items a source using this file format will emit
 * @since Jet 4.4
 */
public class AvroFileFormat<T> implements FileFormat<T> {

    /**
     * Format id for Avro.
     */
    public static final String FORMAT_AVRO = "avro";

    private static final long serialVersionUID = 1L;

    private Class<T> reflectClass;

    /**
     * Creates {@link AvroFileFormat}. See {@link FileFormat#avro} for more
     * details.
     */
    AvroFileFormat() {
    }

    /**
     * Specifies to use reflection to deserialize data into the given class.
     * Jet will use the {@code ReflectDatumReader} to read Avro data. The
     * parameter may be {@code null}, this disables the option to deserialize
     * using reflection.
     *
     * @param reflectClass class to deserialize data into
     */
    @Nonnull
    public AvroFileFormat<T> withReflect(@Nullable Class<T> reflectClass) {
        this.reflectClass = reflectClass;
        return this;
    }

    /**
     * Returns the class Jet will deserialize data into (using reflection).
     * Null if not set.
     */
    @Nullable
    public Class<T> reflectClass() {
        return reflectClass;
    }

    @Nonnull
    @Override
    public String format() {
        return FORMAT_AVRO;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        AvroFileFormat<?> that = (AvroFileFormat<?>) o;
        return Objects.equals(reflectClass, that.reflectClass);
    }

    @Override
    public int hashCode() {
        return Objects.hash(reflectClass);
    }
}
