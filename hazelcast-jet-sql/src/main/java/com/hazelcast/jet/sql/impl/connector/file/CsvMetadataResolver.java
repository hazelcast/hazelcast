/*
 * Copyright (c) 2008-2021, Hazelcast, Inc. All Rights Reserved.
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

import com.hazelcast.jet.pipeline.file.FileFormat;
import com.hazelcast.jet.sql.impl.extract.CsvQueryTarget;
import com.hazelcast.jet.sql.impl.schema.MappingField;

import javax.annotation.Nonnull;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

final class CsvMetadataResolver extends MetadataResolver<Map<String, String>> {

    static final CsvMetadataResolver INSTANCE = new CsvMetadataResolver();

    private static final FileFormat<?> SAMPLE_FORMAT = FileFormat.csv(Map.class);

    @Override
    protected FileFormat<?> sampleFormat() {
        return SAMPLE_FORMAT;
    }

    @Override
    protected List<MappingField> resolveFieldsFromSample(Map<String, String> sample) {
        return CsvResolver.resolveFields(sample.keySet());
    }

    @Override
    protected Metadata resolveMetadata(List<MappingField> resolvedFields, Map<String, ?> options) {
        List<String> fieldNames = createFieldList(resolvedFields);
        FileFormat<String[]> format = FileFormat.csv(createFieldList(resolvedFields));
        return new Metadata(
                toFields(resolvedFields),
                toProcessorMetaSupplierProvider(options, format),
                () -> new CsvQueryTarget(fieldNames));
    }

    @Nonnull
    private static List<String> createFieldList(List<MappingField> resolvedFields) {
        return resolvedFields.stream()
                      .map(field -> field.externalName() != null ? field.externalName() : field.name())
                      .distinct()
                      .collect(Collectors.toList());
    }
}
