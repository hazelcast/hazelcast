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
import java.util.List;
import java.util.Objects;

/**
 * {@link FileFormat} for CSV files. See {@link FileFormat#csv} for more
 * details.
 *
 * @param <T> type of items a source using this file format will emit
 * @since Jet 4.4
 */
public class CsvFileFormat<T> implements FileFormat<T> {

    /**
     * Format ID for CSV.
     */
    public static final String FORMAT_CSV = "csv";

    private static final long serialVersionUID = 1L;

    private final Class<T> clazz;
    private final List<String> fieldNames;

    /**
     * Creates {@link CsvFileFormat}. See {@link FileFormat#csv} for more
     * details.
     */
    CsvFileFormat(@Nonnull Class<T> clazz) {
        this.clazz = clazz;
        this.fieldNames = null;
    }

    /**
     * Creates {@link CsvFileFormat}. See {@link FileFormat#csv} for more
     * details.
     */
    @SuppressWarnings("unchecked")
    CsvFileFormat(@Nullable List<String> fieldNames) {
        this.clazz = (Class<T>) String[].class;
        this.fieldNames = fieldNames;
    }

    @Nonnull
    @Override
    public String format() {
        return FORMAT_CSV;
    }

    /**
     * Returns the class Jet will deserialize data into.
     */
    @Nonnull
    public Class<T> clazz() {
        return clazz;
    }

    /**
     * Return the desired list of fields that is used with {@code String[]}
     * class.
     */
    @Nullable
    public List<String> fieldNames() {
        return fieldNames;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        CsvFileFormat<?> that = (CsvFileFormat<?>) o;
        return Objects.equals(clazz, that.clazz) && Objects.equals(fieldNames, that.fieldNames);
    }

    @Override
    public int hashCode() {
        return Objects.hash(clazz, fieldNames);
    }
}
