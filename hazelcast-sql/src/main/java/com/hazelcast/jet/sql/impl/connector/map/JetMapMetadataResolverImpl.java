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

package com.hazelcast.jet.sql.impl.connector.map;

import com.hazelcast.jet.sql.impl.connector.keyvalue.KvMetadata;
import com.hazelcast.jet.sql.impl.connector.keyvalue.KvMetadataJavaResolver;
import com.hazelcast.jet.sql.impl.schema.MappingField;
import com.hazelcast.nio.serialization.ClassDefinition;
import com.hazelcast.sql.impl.schema.map.JetMapMetadataResolver;

import javax.annotation.Nonnull;
import java.util.List;

import static java.util.Collections.emptyList;
import static java.util.stream.Collectors.toList;

public final class JetMapMetadataResolverImpl implements JetMapMetadataResolver {

    public static final JetMapMetadataResolverImpl INSTANCE = new JetMapMetadataResolverImpl();

    private JetMapMetadataResolverImpl() {
    }

    @Override
    public Object resolveClass(Class<?> clazz, boolean key) {
        List<MappingField> mappingFields = KvMetadataJavaResolver.INSTANCE.resolveFields(key, emptyList(), clazz)
                .collect(toList());
        KvMetadata metadata = KvMetadataJavaResolver.INSTANCE.resolveMetadata(key, mappingFields, clazz);
        return metadata.getUpsertTargetDescriptor();
    }

    @Override
    public Object resolvePortable(@Nonnull ClassDefinition classDef, boolean key) {
        List<MappingField> mappingFields = MetadataPortableResolver.INSTANCE.resolveFields(key, classDef)
                .collect(toList());
        KvMetadata metadata = MetadataPortableResolver.INSTANCE.resolveMetadata(key, mappingFields, classDef);
        return metadata.getUpsertTargetDescriptor();
    }
}
