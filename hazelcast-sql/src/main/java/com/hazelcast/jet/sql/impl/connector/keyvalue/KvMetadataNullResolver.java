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

package com.hazelcast.jet.sql.impl.connector.keyvalue;

import com.hazelcast.internal.serialization.Data;
import com.hazelcast.internal.serialization.InternalSerializationService;
import com.hazelcast.jet.sql.impl.inject.UpsertInjector;
import com.hazelcast.jet.sql.impl.inject.UpsertTarget;
import com.hazelcast.jet.sql.impl.inject.UpsertTargetDescriptor;
import com.hazelcast.sql.impl.schema.MappingField;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.query.impl.getters.Extractors;
import com.hazelcast.sql.impl.extract.QueryExtractor;
import com.hazelcast.sql.impl.extract.QueryTarget;
import com.hazelcast.sql.impl.extract.QueryTargetDescriptor;
import com.hazelcast.sql.impl.type.QueryDataType;

import javax.annotation.Nullable;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

import static java.util.Collections.emptyList;

/**
 * A resolver for "null" serialization format that resolves zero columns.
 */
public class KvMetadataNullResolver implements KvMetadataResolver {

    public static final KvMetadataNullResolver INSTANCE = new KvMetadataNullResolver();

    @Override
    public Stream<MappingField> resolveAndValidateFields(
            boolean isKey, List<MappingField> userFields,
            Map<String, String> options,
            InternalSerializationService serializationService
    ) {
        return Stream.empty();
    }

    @Override
    public KvMetadata resolveMetadata(
            boolean isKey,
            List<MappingField> resolvedFields,
            Map<String, String> options,
            InternalSerializationService serializationService
    ) {
        return new KvMetadata(emptyList(), NullQueryTargetDescriptor.INSTANCE, NullUpsertTargetDescriptor.INSTANCE);
    }

    @Override
    public Stream<String> supportedFormats() {
        return Stream.of((String) null);
    }

    private static class NullQueryTargetDescriptor implements QueryTargetDescriptor {

        private static final NullQueryTargetDescriptor INSTANCE = new NullQueryTargetDescriptor();

        @Override
        public QueryTarget create(InternalSerializationService ss, Extractors extractors, boolean isKey) {
            return new NullQueryTarget();
        }

        @Override
        public void writeData(ObjectDataOutput out) { }

        @Override
        public void readData(ObjectDataInput in) { }

        @Override
        public boolean equals(Object obj) {
            return obj instanceof NullQueryTargetDescriptor;
        }

        @Override
        public int hashCode() {
            return 0;
        }
    }

    private static class NullQueryTarget implements QueryTarget {
        @Override
        public void setTarget(Object target, Data targetData) { }

        @Override
        public QueryExtractor createExtractor(String path, QueryDataType type) {
            throw new IllegalStateException("NullQueryTarget doesn't support this operation");
        }
    }

    private static class NullUpsertTargetDescriptor implements UpsertTargetDescriptor {

        private static final NullUpsertTargetDescriptor INSTANCE = new NullUpsertTargetDescriptor();

        @Override
        public UpsertTarget create(InternalSerializationService serializationService) {
            return new NullUpsertTarget();
        }

        @Override
        public void writeData(ObjectDataOutput out) { }

        @Override
        public void readData(ObjectDataInput in) { }

        @Override
        public boolean equals(Object obj) {
            return obj instanceof NullUpsertTargetDescriptor;
        }

        @Override
        public int hashCode() {
            return 0;
        }
    }

    private static class NullUpsertTarget implements UpsertTarget {
        @Override
        public UpsertInjector createInjector(@Nullable String path, QueryDataType type) {
            throw new IllegalStateException("NullQueryTarget doesn't support this operation");
        }

        @Override
        public void init() { }

        @Override
        public Object conclude() {
            return null;
        }
    }
}
