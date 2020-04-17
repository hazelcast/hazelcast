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
import com.hazelcast.internal.util.BiTuple;
import com.hazelcast.nio.serialization.ClassDefinition;
import com.hazelcast.nio.serialization.FieldType;
import com.hazelcast.sql.impl.extract.GenericQueryTargetDescriptor;
import com.hazelcast.sql.impl.extract.JavaClassQueryTargetDescriptor;
import com.hazelcast.sql.impl.extract.PortableQueryTargetDescriptor;
import com.hazelcast.sql.impl.extract.QueryPath;
import com.hazelcast.sql.impl.extract.QueryTargetDescriptor;
import com.hazelcast.sql.impl.type.QueryDataType;
import com.hazelcast.sql.impl.type.QueryDataTypeUtils;

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
        Map<String, SqlTableField> fieldMap = new LinkedHashMap<>();

        QueryTargetDescriptor valueExtractorDescriptor = resolve(value, false, fieldMap);
        QueryTargetDescriptor keyExtractorDescriptor = resolve(key, true, fieldMap);

        if (keyExtractorDescriptor == null || valueExtractorDescriptor == null) {
            // TODO: This may happen due to serialization problem (e.g. getting Portable class definition).
            return null;
        }

        List<SqlTableField> fields = new ArrayList<>(fieldMap.values());

        return new SqlTableSchema(getSchemaName(), mapName, keyExtractorDescriptor, valueExtractorDescriptor, fields);
    }

    private QueryTargetDescriptor resolve(Object target, boolean isKey, Map<String, SqlTableField> fieldMap) {
        try {
            if (target instanceof Data) {
                Data data = (Data) target;

                if (data.isPortable()) {
                    return resolvePortable(ss.getPortableContext().lookupClassDefinition(data), isKey, fieldMap);
                } else if (data.isJson()) {
                    // TODO: Is it worth? May be just allow for flexible schema which is filled on demand with LATE fields?
                    throw new UnsupportedOperationException("JSON is not supported.");
                } else {
                    return resolveClass(ss.toObject(data).getClass(), isKey, fieldMap, false);
                }
            } else {
                return resolveClass(target.getClass(), isKey, fieldMap, true);
            }
        } catch (Exception e) {
            // TODO: Is it ok to ignore this exception (cannot get Portable class def, cannot serialize)?
            return null;
        }
    }

    private QueryTargetDescriptor resolveClass(
        Class<?> clazz,
        boolean isKey,
        Map<String, SqlTableField> fields,
        boolean objectFormat
    ) {
        // Add top-level object.
        String topName = isKey ? QueryPath.KEY : QueryPath.VALUE;
        QueryDataType topType = QueryDataTypeUtils.resolveTypeForClass(clazz);

        fields.put(topName, new SqlTableField(topName, topName, topType));

        // Add fields.
        // TODO: getDeclaredMethods?
        // TODO: Use only public ones
        for (Method method : clazz.getMethods()) {
            Class<?> returnType = method.getReturnType();
            if (returnType == void.class || returnType == Void.class) {
                continue;
            }

            String name = extractAttributeNameFromMethod(method);

            if (name == null) {
                continue;
            }

            QueryDataType type = QueryDataTypeUtils.resolveTypeForClass(returnType);

            fields.putIfAbsent(name, new SqlTableField(name, resolvePath(name, isKey), type));
        }

        if (objectFormat) {
            return new JavaClassQueryTargetDescriptor(clazz.getName());
        } else {
            return GenericQueryTargetDescriptor.INSTANCE;
        }
    }

    private QueryTargetDescriptor resolvePortable(ClassDefinition clazz, boolean isKey, Map<String, SqlTableField> fields) {
        // Add top-level object.
        String topName = isKey ? QueryPath.KEY : QueryPath.VALUE;
        fields.put(topName, new SqlTableField(topName, topName, QueryDataType.OBJECT));

        // Add fields.
        for (String name : clazz.getFieldNames()) {
            FieldType portableType = clazz.getFieldType(name);

            QueryDataType type = resolvePortableType(portableType);

            fields.putIfAbsent(name, new SqlTableField(name, resolvePath(name, isKey), type));
        }

        return new PortableQueryTargetDescriptor(clazz.getFactoryId(), clazz.getClassId());
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

    @SuppressWarnings("checkstyle:ReturnCount")
    private static QueryDataType resolvePortableType(FieldType portableType) {
        switch (portableType) {
            case BOOLEAN:
                return QueryDataType.BOOLEAN;

            case BYTE:
                return QueryDataType.TINYINT;

            case SHORT:
                return QueryDataType.SMALLINT;

            case CHAR:
            case UTF:
                return QueryDataType.VARCHAR;

            case INT:
                return QueryDataType.INT;

            case LONG:
                return QueryDataType.BIGINT;

            case FLOAT:
                return QueryDataType.REAL;

            case DOUBLE:
                return QueryDataType.DOUBLE;

            default:
                return QueryDataType.OBJECT;
        }
    }

    private static String resolvePath(String name, boolean isKey) {
        return isKey ? QueryPath.KEY + "." + name : name;
    }
}
