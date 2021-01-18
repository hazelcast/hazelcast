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

package com.hazelcast.jet.sql.impl.connector.file;

import com.hazelcast.jet.core.ProcessorMetaSupplier;
import com.hazelcast.jet.pipeline.file.FileFormat;
import com.hazelcast.jet.pipeline.file.FileSourceBuilder;
import com.hazelcast.jet.pipeline.file.FileSources;
import com.hazelcast.jet.pipeline.file.impl.FileProcessorMetaSupplier;
import com.hazelcast.jet.pipeline.file.impl.FileTraverser;
import com.hazelcast.jet.sql.impl.schema.MappingField;
import com.hazelcast.sql.impl.QueryException;
import com.hazelcast.sql.impl.schema.TableField;

import java.util.List;
import java.util.Map;
import java.util.function.Supplier;

import static com.hazelcast.jet.impl.util.ExceptionUtil.sneakyThrow;
import static com.hazelcast.jet.impl.util.Util.toList;
import static com.hazelcast.jet.sql.impl.connector.file.FileSqlConnector.OPTION_GLOB;
import static com.hazelcast.jet.sql.impl.connector.file.FileSqlConnector.OPTION_IGNORE_FILE_NOT_FOUND;
import static com.hazelcast.jet.sql.impl.connector.file.FileSqlConnector.OPTION_PATH;
import static com.hazelcast.jet.sql.impl.connector.file.FileSqlConnector.OPTION_SHARED_FILE_SYSTEM;
import static java.util.Map.Entry;

abstract class MetadataResolver<T> {

    String supportedFormat() {
        return sampleFormat().format();
    }

    List<MappingField> resolveAndValidateFields(List<MappingField> userFields, Map<String, ?> options) {
        return !userFields.isEmpty() ? validateFields(userFields) : resolveFieldsFromSample(options);
    }

    private List<MappingField> validateFields(List<MappingField> userFields) {
        for (MappingField userField : userFields) {
            String externalName = userField.externalName();
            if (externalName != null && externalName.indexOf('.') >= 0) {
                throw QueryException.error("Invalid field external name - '" + externalName +
                                           "'. Nested fields are not supported.");
            }
        }
        return userFields;
    }

    @SuppressWarnings("unchecked")
    private List<MappingField> resolveFieldsFromSample(Map<String, ?> options) {
        FileProcessorMetaSupplier<T> fileProcessorMetaSupplier =
                (FileProcessorMetaSupplier<T>) toProcessorMetaSupplierProvider(options, sampleFormat()).get();

        try (FileTraverser<T> traverser = fileProcessorMetaSupplier.traverser()) {
            T sample = traverser.next();
            if (sample == null) {
                throw QueryException.error("No sample found to resolve the columns");
            }
            return resolveFieldsFromSample(sample);
        } catch (Exception e) {
            throw sneakyThrow(e);
        }
    }

    protected abstract Metadata resolveMetadata(List<MappingField> resolvedFields, Map<String, ?> options);

    protected List<TableField> toFields(List<MappingField> resolvedFields) {
        return toList(
                resolvedFields,
                field -> new FileTableField(
                        field.name(),
                        field.type(),
                        field.externalName() == null ? field.name() : field.externalName()
                )
        );
    }

    @SuppressWarnings("unchecked")
    protected Supplier<ProcessorMetaSupplier> toProcessorMetaSupplierProvider(
            Map<String, ?> options, FileFormat<?> format
    ) {
        return () -> {
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

            for (Entry<String, ?> entry : options.entrySet()) {
                String key = entry.getKey();
                if (OPTION_PATH.equals(key) || OPTION_GLOB.equals(key) || OPTION_SHARED_FILE_SYSTEM.equals(key) ||
                    OPTION_IGNORE_FILE_NOT_FOUND.equals(key)) {
                    continue;
                }

                Object value = entry.getValue();
                if (value instanceof String) {
                    builder.option(key, (String) value);
                } else if (value instanceof Map) {
                    for (Entry<String, String> option : ((Map<String, String>) value).entrySet()) {
                        builder.option(option.getKey(), option.getValue());
                    }
                } else {
                    throw new IllegalArgumentException("Unexpected option type: " + value.getClass());
                }
            }
            return builder.buildMetaSupplier();
        };
    }

    protected abstract FileFormat<?> sampleFormat();

    protected abstract List<MappingField> resolveFieldsFromSample(T sample);
}
