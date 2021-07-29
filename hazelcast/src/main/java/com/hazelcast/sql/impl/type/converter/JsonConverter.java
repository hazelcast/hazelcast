package com.hazelcast.sql.impl.type.converter;

import com.hazelcast.core.HazelcastJsonValue;
import com.hazelcast.sql.impl.type.QueryDataTypeFamily;

public class JsonConverter extends Converter {
    public static final JsonConverter INSTANCE = new JsonConverter();

    public JsonConverter() {
        super(ID_JSON, QueryDataTypeFamily.JSON);
    }

    @Override
    public Class<?> getValueClass() {
        return HazelcastJsonValue.class;
    }

    @Override
    public Object convertToSelf(final Converter converter, final Object val) {
        return null;
    }
}
