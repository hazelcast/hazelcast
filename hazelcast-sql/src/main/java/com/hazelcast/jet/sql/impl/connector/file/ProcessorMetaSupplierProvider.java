/*
 * Copyright 2021 Hazelcast Inc.
 *
 * Licensed under the Hazelcast Community License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://hazelcast.com/hazelcast-community-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.hazelcast.jet.sql.impl.connector.file;

import com.hazelcast.jet.core.ProcessorMetaSupplier;
import com.hazelcast.jet.pipeline.file.FileFormat;
import com.hazelcast.jet.pipeline.file.FileSourceBuilder;
import com.hazelcast.jet.pipeline.file.FileSources;

import java.util.Map;
import java.util.Objects;
import java.util.function.Supplier;

import static com.hazelcast.jet.sql.impl.connector.file.FileSqlConnector.OPTION_GLOB;
import static com.hazelcast.jet.sql.impl.connector.file.FileSqlConnector.OPTION_IGNORE_FILE_NOT_FOUND;
import static com.hazelcast.jet.sql.impl.connector.file.FileSqlConnector.OPTION_PATH;
import static com.hazelcast.jet.sql.impl.connector.file.FileSqlConnector.OPTION_SHARED_FILE_SYSTEM;

class ProcessorMetaSupplierProvider implements Supplier<ProcessorMetaSupplier> {

    private final Map<String, ?> options;
    private final FileFormat<?> format;

    ProcessorMetaSupplierProvider(Map<String, ?> options, FileFormat<?> format) {
        this.options = options;
        this.format = format;
    }

    @Override
    @SuppressWarnings("unchecked")
    public ProcessorMetaSupplier get() {
        FileSourceBuilder<?> builder = FileSources.files((String) options.get(OPTION_PATH)).format(format);

        String glob = (String) options.get(OPTION_GLOB);
        if (glob != null) {
            builder.glob(glob);
        }

        String sharedFileSystem = (String) options.get(OPTION_SHARED_FILE_SYSTEM);
        if (sharedFileSystem != null) {
            builder.sharedFileSystem(Boolean.parseBoolean(sharedFileSystem));
        }

        String ignoreFileNotFound = (String) options.get(OPTION_IGNORE_FILE_NOT_FOUND);
        if (ignoreFileNotFound != null) {
            builder.ignoreFileNotFound(Boolean.parseBoolean(ignoreFileNotFound));
        }

        for (Map.Entry<String, ?> entry : options.entrySet()) {
            String key = entry.getKey();
            if (OPTION_PATH.equals(key) || OPTION_GLOB.equals(key) || OPTION_SHARED_FILE_SYSTEM.equals(key) ||
                    OPTION_IGNORE_FILE_NOT_FOUND.equals(key)) {
                continue;
            }

            Object value = entry.getValue();
            if (value instanceof String) {
                builder.option(key, (String) value);
            } else if (value instanceof Map) {
                for (Map.Entry<String, String> option : ((Map<String, String>) value).entrySet()) {
                    builder.option(option.getKey(), option.getValue());
                }
            } else {
                throw new IllegalArgumentException("Unexpected option type: " + value.getClass());
            }
        }
        return builder.buildMetaSupplier();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        ProcessorMetaSupplierProvider that = (ProcessorMetaSupplierProvider) o;
        return Objects.equals(options, that.options) && Objects.equals(format, that.format);
    }

    @Override
    public int hashCode() {
        return Objects.hash(options, format);
    }
}
