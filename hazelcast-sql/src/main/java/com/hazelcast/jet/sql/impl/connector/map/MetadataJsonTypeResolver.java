package com.hazelcast.jet.sql.impl.connector.map;

import com.hazelcast.internal.serialization.InternalSerializationService;
import com.hazelcast.jet.sql.impl.connector.keyvalue.KvMetadata;
import com.hazelcast.jet.sql.impl.connector.keyvalue.KvMetadataResolver;
import com.hazelcast.jet.sql.impl.extract.HazelcastJsonQueryTargetDescriptor;
import com.hazelcast.jet.sql.impl.inject.HazelcastJsonUpsertTargetDescriptor;
import com.hazelcast.sql.impl.schema.MappingField;
import com.hazelcast.sql.impl.QueryException;
import com.hazelcast.sql.impl.extract.QueryPath;
import com.hazelcast.sql.impl.schema.TableField;
import com.hazelcast.sql.impl.schema.map.MapTableField;
import com.hazelcast.sql.impl.type.QueryDataType;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

import static com.hazelcast.jet.sql.impl.connector.SqlConnector.JSON_TYPE_FORMAT;
import static com.hazelcast.jet.sql.impl.connector.keyvalue.KvMetadataResolver.extractFields;

final class MetadataJsonTypeResolver implements KvMetadataResolver {
    static final MetadataJsonTypeResolver INSTANCE = new MetadataJsonTypeResolver();

    private MetadataJsonTypeResolver() {
    }

    @Override
    public Stream<String> supportedFormats() {
        return Stream.of(JSON_TYPE_FORMAT);
    }

    @Override
    public Stream<MappingField> resolveAndValidateFields(
            boolean isKey,
            List<MappingField> userFields,
            Map<String, String> options,
            InternalSerializationService serializationService
    ) {
        if (isKey) {
            return extractFields(userFields, isKey).entrySet().stream()
                    .map(entry -> {
                        QueryPath path = entry.getKey();
                        if (path.getPath() == null) {
                            throw QueryException.error("Cannot use the '" + path + "' field with JSON serialization");
                        }
                        return entry.getValue();
                    });
        } else {
            return Stream.of(new MappingField("this", QueryDataType.JSON, "this"));
        }
    }

    @Override
    public KvMetadata resolveMetadata(
            boolean isKey,
            List<MappingField> resolvedFields,
            Map<String, String> options,
            InternalSerializationService serializationService
    ) {
        List<TableField> fields = Collections.singletonList(
                new MapTableField("this", QueryDataType.JSON, false, QueryPath.VALUE_PATH)
        );
        return new KvMetadata(
                fields,
                HazelcastJsonQueryTargetDescriptor.INSTANCE,
                HazelcastJsonUpsertTargetDescriptor.INSTANCE
        );
    }
}
