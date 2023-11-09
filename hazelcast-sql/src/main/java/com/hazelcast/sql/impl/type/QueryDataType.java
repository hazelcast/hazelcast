/*
 * Copyright 2023 Hazelcast Inc.
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

package com.hazelcast.sql.impl.type;

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;
import com.hazelcast.sql.impl.SqlDataSerializerHook;
import com.hazelcast.sql.impl.schema.type.TypeKind;
import com.hazelcast.sql.impl.type.converter.BigDecimalConverter;
import com.hazelcast.sql.impl.type.converter.BigIntegerConverter;
import com.hazelcast.sql.impl.type.converter.BooleanConverter;
import com.hazelcast.sql.impl.type.converter.ByteConverter;
import com.hazelcast.sql.impl.type.converter.CalendarConverter;
import com.hazelcast.sql.impl.type.converter.CharacterConverter;
import com.hazelcast.sql.impl.type.converter.Converter;
import com.hazelcast.sql.impl.type.converter.Converters;
import com.hazelcast.sql.impl.type.converter.DateConverter;
import com.hazelcast.sql.impl.type.converter.DoubleConverter;
import com.hazelcast.sql.impl.type.converter.FloatConverter;
import com.hazelcast.sql.impl.type.converter.InstantConverter;
import com.hazelcast.sql.impl.type.converter.IntegerConverter;
import com.hazelcast.sql.impl.type.converter.IntervalConverter;
import com.hazelcast.sql.impl.type.converter.JsonConverter;
import com.hazelcast.sql.impl.type.converter.LocalDateConverter;
import com.hazelcast.sql.impl.type.converter.LocalDateTimeConverter;
import com.hazelcast.sql.impl.type.converter.LocalTimeConverter;
import com.hazelcast.sql.impl.type.converter.LongConverter;
import com.hazelcast.sql.impl.type.converter.MapConverter;
import com.hazelcast.sql.impl.type.converter.NullConverter;
import com.hazelcast.sql.impl.type.converter.ObjectConverter;
import com.hazelcast.sql.impl.type.converter.OffsetDateTimeConverter;
import com.hazelcast.sql.impl.type.converter.RowConverter;
import com.hazelcast.sql.impl.type.converter.ShortConverter;
import com.hazelcast.sql.impl.type.converter.StringConverter;
import com.hazelcast.sql.impl.type.converter.ZonedDateTimeConverter;

import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * Data type represents a type of concrete expression which is based on some basic data type.
 * <p>
 * Java serialization is needed for Jet.
 */
public class QueryDataType implements IdentifiedDataSerializable, Serializable {
    public static final int MAX_DECIMAL_PRECISION = 76;
    public static final int MAX_DECIMAL_SCALE = 38;

    public static final QueryDataType VARCHAR = new QueryDataType(StringConverter.INSTANCE);
    public static final QueryDataType VARCHAR_CHARACTER = new QueryDataType(CharacterConverter.INSTANCE);

    public static final QueryDataType BOOLEAN = new QueryDataType(BooleanConverter.INSTANCE);

    public static final QueryDataType TINYINT = new QueryDataType(ByteConverter.INSTANCE);
    public static final QueryDataType SMALLINT = new QueryDataType(ShortConverter.INSTANCE);
    public static final QueryDataType INT = new QueryDataType(IntegerConverter.INSTANCE);
    public static final QueryDataType BIGINT = new QueryDataType(LongConverter.INSTANCE);
    public static final QueryDataType DECIMAL = new QueryDataType(BigDecimalConverter.INSTANCE);
    public static final QueryDataType DECIMAL_BIG_INTEGER = new QueryDataType(BigIntegerConverter.INSTANCE);
    public static final QueryDataType REAL = new QueryDataType(FloatConverter.INSTANCE);
    public static final QueryDataType DOUBLE = new QueryDataType(DoubleConverter.INSTANCE);

    public static final QueryDataType TIME = new QueryDataType(LocalTimeConverter.INSTANCE);
    public static final QueryDataType DATE = new QueryDataType(LocalDateConverter.INSTANCE);
    public static final QueryDataType TIMESTAMP = new QueryDataType(LocalDateTimeConverter.INSTANCE);
    public static final QueryDataType TIMESTAMP_WITH_TZ_DATE = new QueryDataType(DateConverter.INSTANCE);
    public static final QueryDataType TIMESTAMP_WITH_TZ_CALENDAR = new QueryDataType(CalendarConverter.INSTANCE);
    public static final QueryDataType TIMESTAMP_WITH_TZ_INSTANT = new QueryDataType(InstantConverter.INSTANCE);
    public static final QueryDataType TIMESTAMP_WITH_TZ_OFFSET_DATE_TIME = new QueryDataType(OffsetDateTimeConverter.INSTANCE);
    public static final QueryDataType TIMESTAMP_WITH_TZ_ZONED_DATE_TIME = new QueryDataType(ZonedDateTimeConverter.INSTANCE);

    public static final QueryDataType OBJECT = new QueryDataType(ObjectConverter.INSTANCE);

    public static final QueryDataType NULL = new QueryDataType(NullConverter.INSTANCE);

    public static final QueryDataType INTERVAL_YEAR_MONTH = new QueryDataType(IntervalConverter.YEAR_MONTH);
    public static final QueryDataType INTERVAL_DAY_SECOND = new QueryDataType(IntervalConverter.DAY_SECOND);

    public static final QueryDataType MAP = new QueryDataType(MapConverter.INSTANCE);
    public static final QueryDataType JSON = new QueryDataType(JsonConverter.INSTANCE);
    public static final QueryDataType ROW = new QueryDataType(RowConverter.INSTANCE);

    private Converter converter;
    // nonnull for custom types (nested types)
    private String objectTypeName;
    private TypeKind objectTypeKind = TypeKind.NONE;
    private String objectTypeMetadata;
    private final List<QueryDataTypeField> objectFields = new ArrayList<>();

    public QueryDataType() { }

    public QueryDataType(String objectTypeName) {
        this(objectTypeName, TypeKind.NONE, null);
    }

    public QueryDataType(String objectTypeName, TypeKind typeKind, String typeMetadata) {
        converter = ObjectConverter.INSTANCE;
        this.objectTypeName = objectTypeName;
        objectTypeKind = typeKind;
        objectTypeMetadata = typeMetadata;
    }

    QueryDataType(Converter converter) {
        this.converter = converter;
    }

    public String getObjectTypeName() {
        return objectTypeName;
    }

    public List<QueryDataTypeField> getObjectFields() {
        return objectFields;
    }

    public String getObjectTypeMetadata() {
        return objectTypeMetadata;
    }

    public QueryDataTypeFamily getTypeFamily() {
        return converter.getTypeFamily();
    }

    public Converter getConverter() {
        return converter;
    }

    public TypeKind getObjectTypeKind() {
        return objectTypeKind;
    }

    /**
     * Normalize the given value to a value returned by this instance. If the value doesn't match
     * the type expected by the converter, an exception is thrown.
     *
     * @param value Value
     * @return Normalized value
     * @throws QueryDataTypeMismatchException In case of data type mismatch.
     */
    public Object normalize(Object value) {
        if (value == null) {
            return null;
        }

        Class<?> valueClass = value.getClass();

        if (valueClass == converter.getNormalizedValueClass()) {
            // Do nothing if the value is already in the normalized form.
            return value;
        }

        if (!converter.getValueClass().isAssignableFrom(valueClass)) {
            // Expected and actual class don't match. Throw an error.
            throw new QueryDataTypeMismatchException(converter.getValueClass(), valueClass);
        }

        return converter.convertToSelf(converter, value);
    }

    /**
     * Normalize the given value to a value returned by this instance. If the value doesn't match
     * the type expected by the converter, a conversion is performed.
     *
     * @param value Value
     * @return Normalized value
     */
    public Object convert(Object value) {
        if (value == null) {
            return null;
        }

        Class<?> valueClass = value.getClass();

        if (valueClass == converter.getNormalizedValueClass()) {
            return value;
        }

        return converter.convertToSelf(Converters.getConverter(valueClass), value);
    }

    @Override
    public int getFactoryId() {
        return SqlDataSerializerHook.F_ID;
    }

    @Override
    public int getClassId() {
        return SqlDataSerializerHook.QUERY_DATA_TYPE;
    }

    /**
     * @implNote Collects all distinct custom types into a <em>type map</em> beforehand
     * to avoid infinite recursion. Then, it writes each type with its direct children,
     * i.e. each subtree, in an arbitrary order. {@link #readData} first creates a type
     * map that initially contains only this {@code QueryDataType}, i.e. the root. Then,
     * it reads all subtrees by creating a type only if it is not created before using
     * the type map. Even though subtrees don't lie in a particular order, the children
     * of all subtrees will eventually be populated, including the root.
     */
    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        out.writeInt(converter.getId());
        if (converter.getTypeFamily() != QueryDataTypeFamily.OBJECT) {
            return;
        }

        writeObjectTypeMetadata(this, out);
        if (!isCustomType()) {
            return;
        }

        Map<String, QueryDataType> typeMap = new HashMap<>();
        collectCustomTypes(this, typeMap);

        out.writeInt(typeMap.size());
        for (QueryDataType nestedType : typeMap.values()) {
            writeObjectTypeMetadata(nestedType, out);
            out.writeInt(nestedType.getObjectFields().size());
            for (QueryDataTypeField field : nestedType.getObjectFields()) {
                out.writeString(field.name);
                out.writeInt(field.type.converter.getId());
                writeObjectTypeMetadata(field.type, out);
            }
        }
    }

    private static void writeObjectTypeMetadata(QueryDataType type, ObjectDataOutput out) throws IOException {
        out.writeInt(type.objectTypeKind.value());
        out.writeString(type.objectTypeName);
        out.writeString(type.objectTypeMetadata);
    }

    private void collectCustomTypes(QueryDataType type, Map<String, QueryDataType> typeMap) {
        typeMap.put(type.objectTypeName, type);

        for (QueryDataTypeField field : type.objectFields) {
            if (field.getType().isCustomType()) {
                if (!typeMap.containsKey(field.type.objectTypeName)) {
                    collectCustomTypes(field.type, typeMap);
                }
            }
        }
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        converter = Converters.getConverter(in.readInt());
        if (converter.getTypeFamily() != QueryDataTypeFamily.OBJECT) {
            return;
        }

        readObjectTypeMetadata(this, in);
        if (!isCustomType()) {
            return;
        }

        Map<String, QueryDataType> typeMap = new HashMap<>();
        typeMap.put(objectTypeName, this);

        int typeMapSize = in.readInt();
        for (int i = 0; i < typeMapSize; i++) {
            QueryDataType type = readNestedType(OBJECT.getConverter(), in, typeMap);
            int fields = in.readInt();
            for (int j = 0; j < fields; j++) {
                String fieldName = in.readString();
                Converter converter = Converters.getConverter(in.readInt());
                QueryDataType nestedType = readNestedType(converter, in, typeMap);
                type.getObjectFields().add(new QueryDataTypeField(fieldName, nestedType));
            }
        }
    }

    private static void readObjectTypeMetadata(QueryDataType type, ObjectDataInput in) throws IOException {
        type.objectTypeKind = TypeKind.of(in.readInt());
        type.objectTypeName = in.readString();
        type.objectTypeMetadata = in.readString();
    }

    private static QueryDataType readNestedType(Converter converter, ObjectDataInput in,
                                                Map<String, QueryDataType> typeMap) throws IOException {
        QueryDataType type = new QueryDataType(converter);
        readObjectTypeMetadata(type, in);

        return type.objectTypeName == null
                ? QueryDataTypeUtils.resolveTypeForClass(converter.getValueClass())
                : typeMap.computeIfAbsent(type.objectTypeName, k -> type);
    }

    public boolean isCustomType() {
        return converter.getTypeFamily() == QueryDataTypeFamily.OBJECT && objectTypeName != null;
    }

    @Override
    public int hashCode() {
        return !isCustomType()
                ? converter.getId()
                : Objects.hash(objectTypeName, objectTypeKind, objectTypeMetadata);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        QueryDataType that = (QueryDataType) o;
        return !isCustomType()
                ? converter.getId() == that.converter.getId()
                : objectTypeName.equals(that.objectTypeName)
                        && objectTypeKind == that.objectTypeKind
                        && Objects.equals(objectTypeMetadata, that.objectTypeMetadata)
                        && objectFields.equals(that.objectFields);
    }

    @Override
    public String toString() {
        return getClass().getSimpleName() + " {family=" + getTypeFamily() + "}";
    }

    public static class QueryDataTypeField implements IdentifiedDataSerializable, Serializable {
        private String name;
        private QueryDataType type;

        public QueryDataTypeField() { }

        public QueryDataTypeField(String name, QueryDataType type) {
            this.name = name;
            this.type = type;
        }

        public String getName() {
            return name;
        }

        public QueryDataType getType() {
            return type;
        }

        @Override
        public void writeData(ObjectDataOutput out) throws IOException {
            out.writeString(name);
            out.writeObject(type);
        }

        @Override
        public void readData(ObjectDataInput in) throws IOException {
            name = in.readString();
            type = in.readObject();
        }

        @Override
        public int getFactoryId() {
            return SqlDataSerializerHook.F_ID;
        }

        @Override
        public int getClassId() {
            return SqlDataSerializerHook.QUERY_DATA_TYPE_FIELD;
        }

        @Override
        public int hashCode() {
            return Objects.hash(name, type);
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            QueryDataTypeField that = (QueryDataTypeField) o;
            return name.equals(that.name) && type.equals(that.type);
        }
    }
}
