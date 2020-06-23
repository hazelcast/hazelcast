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

import com.hazelcast.internal.util.StringUtil;
import com.hazelcast.nio.serialization.FieldType;
import com.hazelcast.nio.serialization.GenericRecord;
import com.hazelcast.query.extractor.ValueCallback;
import com.hazelcast.query.extractor.ValueCollector;
import com.hazelcast.query.extractor.ValueReadingException;
import com.hazelcast.query.impl.getters.ExtractorHelper;
import com.hazelcast.query.impl.getters.MultiResult;

import java.io.IOException;
import java.util.LinkedList;
import java.util.ListIterator;

import static com.hazelcast.query.impl.getters.ExtractorHelper.extractArgumentsFromAttributeName;
import static com.hazelcast.query.impl.getters.ExtractorHelper.extractAttributeNameNameWithoutArguments;


public final class GenericRecordQueryReader implements InternalValueReader {

    private final InternalGenericRecord rootRecord;

    public GenericRecordQueryReader(InternalGenericRecord rootRecord) {
        this.rootRecord = rootRecord;
    }

    @SuppressWarnings("unchecked")
    public void read(String path, ValueCallback callback) {
        try {
            Object result = read(path);
            if (result instanceof MultiResult) {
                MultiResult multiResult = (MultiResult) result;
                for (Object singleResult : multiResult.getResults()) {
                    callback.onResult(singleResult);
                }
            } else {
                callback.onResult(result);
            }
        } catch (IOException e) {
            throw new ValueReadingException(e.getMessage(), e);
        } catch (RuntimeException e) {
            throw new ValueReadingException(e.getMessage(), e);
        }
    }


    @SuppressWarnings("unchecked")
    public void read(String path, ValueCollector collector) {
        try {
            Object result = read(path);
            if (result instanceof MultiResult) {
                MultiResult multiResult = (MultiResult) result;
                for (Object singleResult : multiResult.getResults()) {
                    collector.addObject(singleResult);
                }
            } else {
                collector.addObject(result);
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
            begin = end + 1;
            end = StringUtil.indexOf(fieldPath, '.', begin);

            if (path.length() == 0) {
                throw new IllegalArgumentException("The token's length cannot be zero: " + fieldPath);
            }

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

    private <T> T readIndexed(InternalGenericRecord record, String path, int index) throws IOException {
        if (!record.hasField(path)) {
            return null;
        }
        FieldType type = record.getFieldType(path);
        switch (type) {
            case BYTE_ARRAY:
                return (T) record.readByteFromArray(path, index);
            case SHORT_ARRAY:
                return (T) record.readShortFromArray(path, index);
            case INT_ARRAY:
                return (T) record.readIntFromArray(path, index);
            case LONG_ARRAY:
                return (T) record.readLongFromArray(path, index);
            case FLOAT_ARRAY:
                return (T) record.readFloatFromArray(path, index);
            case DOUBLE_ARRAY:
                return (T) record.readDoubleFromArray(path, index);
            case BOOLEAN_ARRAY:
                return (T) record.readBooleanFromArray(path, index);
            case CHAR_ARRAY:
                return (T) record.readCharFromArray(path, index);
            case UTF_ARRAY:
                return (T) record.readUTFFromArray(path, index);
            case PORTABLE_ARRAY:
                return (T) record.readObjectFromArray(path, index);
            default:
                throw new IllegalArgumentException("Unsupported type " + type);
        }
    }

    private <T> T readLeaf(InternalGenericRecord record, String path) throws IOException {
        if (!record.hasField(path)) {
            return null;
        }
        FieldType type = record.getFieldType(path);
        switch (type) {
            case BYTE:
                return (T) Byte.valueOf(record.readByte(path));
            case BYTE_ARRAY:
                return (T) record.readByteArray(path);
            case SHORT:
                return (T) Short.valueOf(record.readShort(path));
            case SHORT_ARRAY:
                return (T) record.readShortArray(path);
            case INT:
                return (T) Integer.valueOf(record.readInt(path));
            case INT_ARRAY:
                return (T) record.readIntArray(path);
            case LONG:
                return (T) Long.valueOf(record.readLong(path));
            case LONG_ARRAY:
                return (T) record.readLongArray(path);
            case FLOAT:
                return (T) Float.valueOf(record.readFloat(path));
            case FLOAT_ARRAY:
                return (T) record.readFloatArray(path);
            case DOUBLE:
                return (T) Double.valueOf(record.readDouble(path));
            case DOUBLE_ARRAY:
                return (T) record.readDoubleArray(path);
            case BOOLEAN:
                return (T) Boolean.valueOf(record.readBoolean(path));
            case BOOLEAN_ARRAY:
                return (T) record.readBooleanArray(path);
            case CHAR:
                return (T) Character.valueOf(record.readChar(path));
            case CHAR_ARRAY:
                return (T) record.readCharArray(path);
            case UTF:
                return (T) record.readUTF(path);
            case UTF_ARRAY:
                return (T) record.readUTFArray(path);
            case PORTABLE:
                return (T) record.readObject(path);
            case PORTABLE_ARRAY:
                return (T) record.readObjectArray(path);
            default:
                throw new IllegalArgumentException("Unsupported type " + type);
        }
    }

}
