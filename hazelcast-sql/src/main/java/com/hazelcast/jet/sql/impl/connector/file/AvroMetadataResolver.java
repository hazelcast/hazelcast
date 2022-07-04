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
import com.hazelcast.jet.sql.impl.extract.AvroQueryTarget;
import com.hazelcast.sql.impl.schema.MappingField;
import org.apache.avro.generic.GenericRecord;

import java.util.List;
import java.util.Map;

final class AvroMetadataResolver extends MetadataResolver<GenericRecord> {

    static final AvroMetadataResolver INSTANCE = new AvroMetadataResolver();

    private static final FileFormat<Map<String, String>> FORMAT = FileFormat.avro();

    @Override
    protected String supportedFormat() {
        return sampleFormat().format();
    }

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
                new ProcessorMetaSupplierProvider(options, FORMAT),
                AvroQueryTarget::new);
    }
}
