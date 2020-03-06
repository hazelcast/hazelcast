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

package com.hazelcast.sql.impl.schema;

import com.hazelcast.core.DistributedObject;
import com.hazelcast.internal.serialization.Data;
import com.hazelcast.internal.serialization.InternalSerializationService;
import com.hazelcast.internal.serialization.PortableContext;
import com.hazelcast.internal.util.BiTuple;
import com.hazelcast.nio.serialization.ClassDefinition;
import com.hazelcast.nio.serialization.FieldType;
import com.hazelcast.query.QueryConstants;
import com.hazelcast.sql.impl.type.DataType;
import com.hazelcast.sql.impl.type.DataTypeUtils;

import java.io.IOException;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

public abstract class MapSqlSchemaResolver implements SqlSchemaResolver {
    private static final String METHOD_PREFIX_GET = "get";
    private static final String METHOD_PREFIX_IS = "is";
    private static final String METHOD_GET_CLASS = "getClass";

    /** Serialization service. */
    protected final InternalSerializationService ss;

    protected MapSqlSchemaResolver(InternalSerializationService ss) {
        this.ss = ss;
    }

    @Override
    public SqlTableSchema resolve(DistributedObject object) {
        BiTuple<Data, Object> sample = getSample(object);

        if (sample == null) {
            return null;
        }

        return resolveFromSample(object.getName(), sample.element1(), sample.element2());
    }

    protected abstract String getSchemaName();

    protected abstract BiTuple<Data, Object> getSample(DistributedObject object);

    private SqlTableSchema resolveFromSample(String mapName, Data key, Object value) {
        List<SqlTableField> fields;

        if (key.isPortable()) {
            fields = resolvePortableFields(key, value);
        } else if (key.isJson()) {
            throw new UnsupportedOperationException("JSON is not supported at the moment.");
        } else {
            fields = resolveObjectFields(key, value);
        }

        return new SqlTableSchema(getSchemaName(), mapName, fields);
    }

    private List<SqlTableField> resolveObjectFields(Data key, Object value) {
        Object keyObject = ss.toObject(key);
        Object valueObject = value instanceof Data ? ss.toObject(value) : value;

        Map<String, SqlTableField> fields = new LinkedHashMap<>();

        resolveObjectFields(fields, valueObject, false);
        resolveObjectFields(fields, keyObject, true);

        return new ArrayList<>(fields.values());
    }

    private void resolveObjectFields(Map<String, SqlTableField> fields, Object object, boolean isKey) {
        // Add predefined fields.
        String topName = isKey ? QueryConstants.KEY_ATTRIBUTE_NAME.value() : QueryConstants.THIS_ATTRIBUTE_NAME.value();
        DataType topType = DataTypeUtils.resolveTypeOrNull(object.getClass());

        fields.put(topName, new SqlTableField(topName, topName, topType));

        // Analyze getters.
        for (Method method : object.getClass().getMethods()) {
            String name = extractAttributeNameFromMethod(method);

            if (name == null) {
                continue;
            }

            DataType type = DataTypeUtils.resolveTypeOrNull(method.getReturnType());

            fields.putIfAbsent(name, new SqlTableField(name, resolvePath(name, isKey), type));
        }
    }

    private static String extractAttributeNameFromMethod(Method method) {
        String methodName = method.getName();

        if (methodName.equals(METHOD_GET_CLASS)) {
            return null;
        }

        String fieldNameWithWrongCase;

        if (methodName.startsWith(METHOD_PREFIX_GET) && methodName.length() > METHOD_PREFIX_GET.length()) {
            fieldNameWithWrongCase = methodName.substring(METHOD_PREFIX_GET.length());
        } else if (methodName.startsWith(METHOD_PREFIX_IS) && methodName.length() > METHOD_PREFIX_IS.length()) {
            fieldNameWithWrongCase = methodName.substring(METHOD_PREFIX_IS.length());
        } else {
            return null;
        }

        return Character.toLowerCase(fieldNameWithWrongCase.charAt(0)) + fieldNameWithWrongCase.substring(1);
    }

    private List<SqlTableField> resolvePortableFields(Data key, Object value) {
        // TODO: Need to ensure that __key and this are added properly.
        assert key.isPortable();
        assert value instanceof Data;

        Map<String, SqlTableField> fields = new LinkedHashMap<>();

        resolvePortableFields(fields, (Data) value, false);
        resolvePortableFields(fields, key, true);

        return new ArrayList<>(fields.values());
    }

    private void resolvePortableFields(Map<String, SqlTableField> fields, Data data, boolean isKey) {
        try {
            PortableContext context = ss.getPortableContext();

            ClassDefinition classDefinition = context.lookupClassDefinition(data);

            for (String name : classDefinition.getFieldNames()) {
                FieldType portableType = classDefinition.getFieldType(name);

                DataType type = resolvePortableType(portableType);

                fields.putIfAbsent(name, new SqlTableField(name, resolvePath(name, isKey), type));
            }
        } catch (IOException ignore) {
            // TODO: Is it ok to ignore this exception?
            // No-op.
        }
    }

    @SuppressWarnings("checkstyle:ReturnCount")
    private static DataType resolvePortableType(FieldType portableType) {
        switch (portableType) {
            case BOOLEAN:
                return DataType.BIT;

            case BYTE:
                return DataType.TINYINT;

            case SHORT:
                return DataType.SMALLINT;

            case CHAR:
            case UTF:
                return DataType.VARCHAR;

            case INT:
                return DataType.INT;

            case LONG:
                return DataType.BIGINT;

            case FLOAT:
                return DataType.REAL;

            case DOUBLE:
                return DataType.DOUBLE;

            default:
                return DataType.OBJECT;
        }
    }

    private static String resolvePath(String name, boolean isKey) {
        return isKey ? QueryConstants.KEY_ATTRIBUTE_NAME.value() + "." + name : name;
    }
}
