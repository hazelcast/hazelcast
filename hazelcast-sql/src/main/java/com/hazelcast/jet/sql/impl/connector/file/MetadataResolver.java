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

import com.hazelcast.jet.pipeline.file.FileFormat;
import com.hazelcast.jet.pipeline.file.impl.FileProcessorMetaSupplier;
import com.hazelcast.jet.pipeline.file.impl.FileTraverser;
import com.hazelcast.sql.impl.QueryException;
import com.hazelcast.sql.impl.schema.MappingField;
import com.hazelcast.sql.impl.schema.TableField;

import java.util.List;
import java.util.Map;

import static com.hazelcast.jet.impl.util.ExceptionUtil.sneakyThrow;
import static com.hazelcast.jet.impl.util.Util.toList;

abstract class MetadataResolver<T> {

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
                (FileProcessorMetaSupplier<T>) new ProcessorMetaSupplierProvider(options, sampleFormat()).get();

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

    protected abstract String supportedFormat();

    protected abstract FileFormat<?> sampleFormat();

    protected abstract List<MappingField> resolveFieldsFromSample(T sample);

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
}
