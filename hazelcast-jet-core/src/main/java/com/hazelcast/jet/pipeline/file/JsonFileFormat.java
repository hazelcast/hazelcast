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

package com.hazelcast.jet.pipeline.file;

import javax.annotation.Nonnull;

import static java.util.Objects.requireNonNull;

/**
 * {@code FileFormat} for the JSON Lines files. See {@link FileFormat#json}
 * for more details.
 *
 * @param <T> type of items a source using this file format will emit
 * @since 4.4
 */
public class JsonFileFormat<T> implements FileFormat<T> {

    /**
     * Format ID for JSON Lines.
     */
    public static final String FORMAT_JSONL = "jsonl";

    private final Class<T> clazz;

    /**
     * Creates a {@code JsonFileFormat}. See {@link FileFormat#json} for
     * more details.
     *
     * @param clazz the type of the object to deserialize JSON into
     */
    JsonFileFormat(@Nonnull Class<T> clazz) {
        this.clazz = requireNonNull(clazz, "class must not be null");
    }

    /**
     * Returns the type of the object the data source using this format will
     * emit.
     */
    @Nonnull
    public Class<T> clazz() {
        return clazz;
    }

    @Nonnull @Override
    public String format() {
        return FORMAT_JSONL;
    }
}
