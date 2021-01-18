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

import com.hazelcast.jet.pipeline.file.FileFormat;
import com.hazelcast.jet.sql.impl.extract.AvroQueryTarget;
import com.hazelcast.jet.sql.impl.schema.MappingField;
import org.apache.avro.generic.GenericRecord;

import java.util.List;
import java.util.Map;

final class AvroMetadataResolver extends MetadataResolver<GenericRecord> {

    static final AvroMetadataResolver INSTANCE = new AvroMetadataResolver();

    private static final FileFormat<Map<String, String>> FORMAT = FileFormat.avro();

    @Override
    protected FileFormat<?> sampleFormat() {
        return FORMAT;
    }

    @Override
    protected List<MappingField> resolveFieldsFromSample(GenericRecord record) {
        return AvroResolver.resolveFields(record.getSchema());
    }

    @Override
    protected Metadata resolveMetadata(List<MappingField> resolvedFields, Map<String, ?> options) {
        return new Metadata(
                toFields(resolvedFields),
                toProcessorMetaSupplierProvider(options, FORMAT),
                AvroQueryTarget::new);
    }
}
