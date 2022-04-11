/*
 * Copyright (c) 2008-2022, Hazelcast, Inc. All Rights Reserved.
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
import com.hazelcast.nio.serialization.FieldKind;
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

import static com.hazelcast.internal.serialization.impl.FieldOperations.fieldOperations;
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
                    InternalGenericRecord subGenericRecord = record.getInternalGenericRecord(fieldName);
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
                    InternalGenericRecord[] genericRecords = record.getArrayOfInternalGenericRecord(fieldName);
                    if (genericRecords == null || genericRecords.length == 0) {
                        multiResult.setNullOrEmptyTarget(true);
                        continue;
                    }
                    for (InternalGenericRecord internalGenericRecord : genericRecords) {
                        if (internalGenericRecord != null) {
                            iterator.add(internalGenericRecord);
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
                    InternalGenericRecord genericRecord = record.getInternalGenericRecordFromArray(fieldName, index);
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
                InternalGenericRecord record = (InternalGenericRecord) iterator.next();
                Object leaf = readIndexed(record, fieldName, index);
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

    private Object readIndexed(InternalGenericRecord record, String path, int index) {
        if (!record.hasField(path)) {
            return null;
        }
        FieldKind kind = record.getFieldKind(path);
        return fieldOperations(kind).readIndexed(record, path, index);
    }

    private Object readLeaf(InternalGenericRecord record, String path) {
        if (!record.hasField(path)) {
            return null;
        }
        FieldKind kind = record.getFieldKind(path);
        return fieldOperations(kind).readAsLeafObjectOnQuery(record, path);
    }

}
