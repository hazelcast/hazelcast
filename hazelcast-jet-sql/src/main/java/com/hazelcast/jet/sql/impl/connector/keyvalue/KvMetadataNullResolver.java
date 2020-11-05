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

package com.hazelcast.jet.sql.impl.connector.keyvalue;

import com.hazelcast.internal.serialization.InternalSerializationService;
import com.hazelcast.jet.sql.impl.inject.UpsertInjector;
import com.hazelcast.jet.sql.impl.inject.UpsertTarget;
import com.hazelcast.jet.sql.impl.inject.UpsertTargetDescriptor;
import com.hazelcast.jet.sql.impl.schema.MappingField;
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

import static java.util.Collections.emptyList;

/**
 * A resolver for "null" serialization format that resolves zero columns.
 */
public class KvMetadataNullResolver implements KvMetadataResolver {

    public static final KvMetadataNullResolver INSTANCE = new KvMetadataNullResolver();

    @Override
    public List<MappingField> resolveAndValidateFields(
            boolean isKey, List<MappingField> userFields,
            Map<String, String> options,
            InternalSerializationService serializationService
    ) {
        return emptyList();
    }

    @Override
    public KvMetadata resolveMetadata(
            boolean isKey,
            List<MappingField> resolvedFields,
            Map<String, String> options,
            InternalSerializationService serializationService
    ) {
        return new KvMetadata(emptyList(), new NullQueryTargetDescriptor(), new NullUpsertTargetDescriptor());
    }

    @Override
    public String supportedFormat() {
        return null;
    }

    private static class NullQueryTargetDescriptor implements QueryTargetDescriptor {
        @Override
        public QueryTarget create(InternalSerializationService ss, Extractors extractors, boolean isKey) {
            return new NullQueryTarget();
        }

        @Override
        public void writeData(ObjectDataOutput out) { }

        @Override
        public void readData(ObjectDataInput in) { }

    }

    private static class NullQueryTarget implements QueryTarget {
        @Override
        public void setTarget(Object target) { }

        @Override
        public QueryExtractor createExtractor(String path, QueryDataType type) {
            throw new IllegalStateException("NullQueryTarget doesn't support this operation");
        }
    }

    private static class NullUpsertTargetDescriptor implements UpsertTargetDescriptor {
        @Override
        public UpsertTarget create(InternalSerializationService serializationService) {
            return new NullUpsertTarget();
        }

        @Override
        public void writeData(ObjectDataOutput out) { }

        @Override
        public void readData(ObjectDataInput in) { }

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
