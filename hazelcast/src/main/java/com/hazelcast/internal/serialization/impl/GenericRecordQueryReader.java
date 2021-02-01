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

package com.hazelcast.internal.serialization.impl;

import com.hazelcast.internal.serialization.impl.portable.PortableInternalGenericRecord;
import com.hazelcast.internal.util.StringUtil;
import com.hazelcast.nio.serialization.FieldType;
import com.hazelcast.nio.serialization.GenericRecord;
import com.hazelcast.query.extractor.ValueCallback;
import com.hazelcast.query.extractor.ValueCollector;
import com.hazelcast.query.extractor.ValueReader;
import com.hazelcast.query.extractor.ValueReadingException;
import com.hazelcast.query.impl.getters.ExtractorHelper;
import com.hazelcast.query.impl.getters.MultiResult;

import java.io.IOException;
import java.util.LinkedList;
import java.util.ListIterator;
import java.util.function.Consumer;

import static com.hazelcast.query.impl.getters.ExtractorHelper.extractArgumentsFromAttributeName;
import static com.hazelcast.query.impl.getters.ExtractorHelper.extractAttributeNameNameWithoutArguments;

/**
 * Reads a field or array of fields from a `InternalGenericRecord` according to given query `path`
 *
 * @see InternalGenericRecord
 * Any format that exposes an `InternalGenericRecord` will benefit from hazelcast query.
 * @see PortableInternalGenericRecord for Portable InternalGenericRecord
 * <p>
 * Example queries
 * "age"
 * "engine.power"
 * "child[0].age"
 * "engine.wheel[0].pressure"
 * "engine.wheel[any].pressure"
 * "top500Companies[0].ceo"
 * "company.employees[any]"
 * "limbs[*].fingers"
 * <p>
 * It also implements ValueReader to support reading into `ValueCallback` and `ValueCollector`
 */
public final class GenericRecordQueryReader implements ValueReader {

    private final InternalGenericRecord rootRecord;

    public GenericRecordQueryReader(InternalGenericRecord rootRecord) {
        this.rootRecord = rootRecord;
    }

    @SuppressWarnings("unchecked")
    public void read(String path, ValueCallback callback) {
        read(path, (ValueCollector) callback::onResult);
    }

    @SuppressWarnings("unchecked")
    public void read(String path, ValueCollector collector) {
        read(path, (Consumer) collector::addObject);
    }

    private void read(String path, Consumer consumer) {
        try {
            Object result = read(path);
            if (result instanceof MultiResult) {
                MultiResult multiResult = (MultiResult) result;
                for (Object singleResult : multiResult.getResults()) {
                    consumer.accept(singleResult);
                }
            } else {
                consumer.accept(result);
            }
        } catch (IOException e) {
            throw new ValueReadingException(e.getMessage(), e);
        } catch (RuntimeException e) {
            throw new ValueReadingException(e.getMessage(), e);
        }
    }

    public Object read(String fieldPath) throws IOException {
        if (fieldPath == null) {
            throw new IllegalArgumentException("field path can not be null");
        }
        if (fieldPath.endsWith(".")) {
            throw new IllegalArgumentException("Malformed path " + fieldPath);
        }

        if (rootRecord.hasField(fieldPath)) {
            return readLeaf(rootRecord, fieldPath);
        }

        LinkedList<Object> results = new LinkedList<>();
        results.add(rootRecord);
        MultiResult<Object> multiResult = new MultiResult<>(results);

        int begin = 0;
        int end = StringUtil.indexOf(fieldPath, '.');
        //handle the paths except leaf
        while (end != -1) {
            String path = fieldPath.substring(begin, end);
            if (path.length() == 0) {
                throw new IllegalArgumentException("The token's length cannot be zero: " + fieldPath);
            }

            begin = end + 1;
            end = StringUtil.indexOf(fieldPath, '.', begin);

            ListIterator<Object> iterator = results.listIterator();
            String fieldName = extractAttributeNameNameWithoutArguments(path);
            if (!path.contains("]")) {
                // ex: attribute
                while (iterator.hasNext()) {
                    InternalGenericRecord record = (InternalGenericRecord) iterator.next();
                    if (!record.hasField(fieldName)) {
                        iterator.remove();
                        multiResult.setNullOrEmptyTarget(true);
                        continue;
                    }
                    InternalGenericRecord subGenericRecord = (InternalGenericRecord) record.readGenericRecord(fieldName);
                    if (subGenericRecord == null) {
                        iterator.remove();
                        multiResult.setNullOrEmptyTarget(true);
                        continue;
                    }
                    iterator.set(subGenericRecord);
                }
            } else if (path.endsWith("[any]")) {
                // ex: attribute any
                while (iterator.hasNext()) {
                    InternalGenericRecord record = (InternalGenericRecord) iterator.next();
                    iterator.remove();
                    if (!record.hasField(fieldName)) {
                        multiResult.setNullOrEmptyTarget(true);
                        continue;
                    }
                    GenericRecord[] genericRecords = record.readGenericRecordArray(fieldName);
                    if (genericRecords == null || genericRecords.length == 0) {
                        multiResult.setNullOrEmptyTarget(true);
                        continue;
                    }
                    for (GenericRecord genericRecord : genericRecords) {
                        if (genericRecord != null) {
                            iterator.add(genericRecord);
                        } else {
                            multiResult.setNullOrEmptyTarget(true);
                        }
                    }
                }
            } else {
                // ex: attribute[2]
                int index = Integer.parseInt(extractArgumentsFromAttributeName(path));
                while (iterator.hasNext()) {
                    InternalGenericRecord record = (InternalGenericRecord) iterator.next();
                    if (!record.hasField(fieldName)) {
                        iterator.remove();
                        multiResult.setNullOrEmptyTarget(true);
                        continue;
                    }
                    GenericRecord genericRecord = record.readGenericRecordFromArray(fieldName, index);
                    if (genericRecord != null) {
                        iterator.set(genericRecord);
                    } else {
                        iterator.remove();
                        multiResult.setNullOrEmptyTarget(true);
                    }
                }
            }
        }

        //last loop that we have skipped
        String path = fieldPath.substring(begin);
        if (path.length() == 0) {
            throw new IllegalArgumentException("The token's length cannot be zero: " + fieldPath);
        }

        ListIterator<Object> iterator = results.listIterator();
        String fieldName = extractAttributeNameNameWithoutArguments(path);
        if (!path.contains("]")) {
            // ex: attribute
            while (iterator.hasNext()) {
                InternalGenericRecord record = (InternalGenericRecord) iterator.next();
                Object leaf = readLeaf(record, fieldName);
                iterator.set(leaf);
            }
        } else if (path.endsWith("[any]")) {
            // ex: attribute any
            while (iterator.hasNext()) {
                InternalGenericRecord record = (InternalGenericRecord) iterator.next();
                iterator.remove();
                Object leaves = readLeaf(record, fieldName);
                if (leaves == null) {
                    multiResult.setNullOrEmptyTarget(true);
                } else if (leaves instanceof Object[]) {
                    Object[] array = (Object[]) leaves;
                    if (array.length == 0) {
                        multiResult.setNullOrEmptyTarget(true);
                        continue;
                    }
                    for (Object leaf : array) {
                        iterator.add(leaf);
                    }
                } else {
                    assert leaves.getClass().isArray() : "parameter is not an array";
                    if (!ExtractorHelper.reducePrimitiveArrayInto(iterator::add, leaves)) {
                        multiResult.setNullOrEmptyTarget(true);
                    }
                }
            }
        } else {
            // ex: attribute[2]
            int index = Integer.parseInt(extractArgumentsFromAttributeName(path));
            while (iterator.hasNext()) {
                GenericRecord record = (GenericRecord) iterator.next();
                Object leaf = readIndexed((InternalGenericRecord) record, fieldName, index);
                iterator.set(leaf);
            }
        }

        if (multiResult.isNullEmptyTarget()) {
            results.addFirst(null);
        } else if (results.size() == 1) {
            return results.get(0);
        }
        return multiResult;
    }

    private Object readIndexed(InternalGenericRecord record, String path, int index) throws IOException {
        if (!record.hasField(path)) {
            return null;
        }
        FieldType type = record.getFieldType(path);
        switch (type) {
            case BYTE_ARRAY:
                return record.readByteFromArray(path, index);
            case SHORT_ARRAY:
                return record.readShortFromArray(path, index);
            case INT_ARRAY:
                return record.readIntFromArray(path, index);
            case LONG_ARRAY:
                return record.readLongFromArray(path, index);
            case FLOAT_ARRAY:
                return record.readFloatFromArray(path, index);
            case DOUBLE_ARRAY:
                return record.readDoubleFromArray(path, index);
            case BOOLEAN_ARRAY:
                return record.readBooleanFromArray(path, index);
            case CHAR_ARRAY:
                return record.readCharFromArray(path, index);
            case UTF_ARRAY:
                return record.readUTFFromArray(path, index);
            case PORTABLE_ARRAY:
                return record.readObjectFromArray(path, index);
            case DECIMAL_ARRAY:
                return record.readDecimalFromArray(path, index);
            case TIME_ARRAY:
                return record.readTimeFromArray(path, index);
            case DATE_ARRAY:
                return record.readDateFromArray(path, index);
            case TIMESTAMP_ARRAY:
                return record.readTimestampFromArray(path, index);
            case TIMESTAMP_WITH_TIMEZONE_ARRAY:
                return record.readTimestampWithTimezoneFromArray(path, index);
            default:
                throw new IllegalArgumentException("Unsupported type " + type);
        }
    }

    private Object readLeaf(InternalGenericRecord record, String path) throws IOException {
        if (!record.hasField(path)) {
            return null;
        }
        FieldType type = record.getFieldType(path);
        switch (type) {
            case BYTE:
                return record.readByte(path);
            case BYTE_ARRAY:
                return record.readByteArray(path);
            case SHORT:
                return record.readShort(path);
            case SHORT_ARRAY:
                return record.readShortArray(path);
            case INT:
                return record.readInt(path);
            case INT_ARRAY:
                return record.readIntArray(path);
            case LONG:
                return record.readLong(path);
            case LONG_ARRAY:
                return record.readLongArray(path);
            case FLOAT:
                return record.readFloat(path);
            case FLOAT_ARRAY:
                return record.readFloatArray(path);
            case DOUBLE:
                return record.readDouble(path);
            case DOUBLE_ARRAY:
                return record.readDoubleArray(path);
            case BOOLEAN:
                return record.readBoolean(path);
            case BOOLEAN_ARRAY:
                return record.readBooleanArray(path);
            case CHAR:
                return record.readChar(path);
            case CHAR_ARRAY:
                return record.readCharArray(path);
            case UTF:
                return record.readUTF(path);
            case UTF_ARRAY:
                return record.readUTFArray(path);
            case PORTABLE:
                return record.readObject(path);
            case PORTABLE_ARRAY:
                return record.readObjectArray(path);
            case DECIMAL:
                return record.readDecimal(path);
            case DECIMAL_ARRAY:
                return record.readDecimalArray(path);
            case TIME:
                return record.readTime(path);
            case TIME_ARRAY:
                return record.readTimeArray(path);
            case DATE:
                return record.readDate(path);
            case DATE_ARRAY:
                return record.readDateArray(path);
            case TIMESTAMP:
                return record.readTimestamp(path);
            case TIMESTAMP_ARRAY:
                return record.readTimestampArray(path);
            case TIMESTAMP_WITH_TIMEZONE:
                return record.readTimestampWithTimezone(path);
            case TIMESTAMP_WITH_TIMEZONE_ARRAY:
                return record.readTimestampWithTimezoneArray(path);
            default:
                throw new IllegalArgumentException("Unsupported type " + type);
        }
    }

}
