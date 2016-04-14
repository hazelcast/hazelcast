package com.hazelcast.query.impl.getters;

import com.hazelcast.internal.serialization.InternalSerializationService;
import com.hazelcast.internal.serialization.PortableContext;
import com.hazelcast.nio.serialization.ClassDefinition;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.nio.serialization.FieldDefinition;
import com.hazelcast.nio.serialization.Portable;
import com.hazelcast.nio.serialization.PortableReader;

final class PortableGetter extends Getter {

    private final InternalSerializationService serializationService;

    public PortableGetter(InternalSerializationService serializationService) {
        super(null);
        this.serializationService = serializationService;
    }

    @Override
    Object getValue(Object target, String fieldPath) throws Exception {
        Data data = (Data) target;
        PortableContext context = serializationService.getPortableContext();
        PortableReader reader = serializationService.createPortableReader(data);
        ClassDefinition classDefinition = context.lookupClassDefinition(data);
        FieldDefinition fieldDefinition = context.getFieldDefinition(classDefinition, fieldPath);

        if (fieldDefinition != null) {
            return reader.read(fieldPath);
        } else {
            return null;
        }
    }

    @Override
    Object getValue(Object obj) throws Exception {
        throw new IllegalArgumentException("Path agnostic value extraction unsupported");
    }

    @Override
    Class getReturnType() {
        return Portable.class;
    }

    @Override
    boolean isCacheable() {
        // Non-cacheable since it's a generic getter and the cache shouldn't be polluted with the same instance
        // for various keys. A singleton should be used instead during getter creation.
        return false;
    }

}
