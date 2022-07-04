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

/**
 * {@link FileFormat} for Parquet files. See {@link FileFormat#parquet} for
 * more details.
 *
 * @param <T> type of items a source using this file format will emit
 * @since Jet 4.4
 */
public class ParquetFileFormat<T> implements FileFormat<T> {

    /**
     * Format ID for Parquet.
     */
    public static final String FORMAT_PARQUET = "parquet";

    private static final long serialVersionUID = 1L;

    /**
     * Creates {@link ParquetFileFormat}. See {@link FileFormat#parquet()}
     * for more details.
     */
    ParquetFileFormat() {
    }

    @Nonnull
    @Override
    public String format() {
        return FORMAT_PARQUET;
    }

    @Override
    public boolean equals(Object obj) {
        return obj instanceof ParquetFileFormat;
    }

    @Override
    public int hashCode() {
        return 0;
    }
}
