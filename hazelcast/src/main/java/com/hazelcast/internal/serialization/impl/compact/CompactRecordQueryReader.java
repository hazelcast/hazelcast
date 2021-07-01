/*
 * Copyright (c) 2008-2021, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.internal.serialization.impl.compact;

import com.hazelcast.internal.util.StringUtil;
import com.hazelcast.nio.serialization.compact.CompactRecord;
import com.hazelcast.nio.serialization.compact.TypeID;
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

import static com.hazelcast.internal.serialization.impl.compact.schema.FieldOperations.fieldOperations;
import static com.hazelcast.query.impl.getters.ExtractorHelper.extractArgumentsFromAttributeName;
import static com.hazelcast.query.impl.getters.ExtractorHelper.extractAttributeNameNameWithoutArguments;

/**
 * Reads a field or array of fields from a `InternalCompactRecord` according to given query `path`
 *
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
public final class CompactRecordQueryReader implements ValueReader {

    private final InternalCompactRecord rootRecord;

    public CompactRecordQueryReader(InternalCompactRecord rootRecord) {
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
                    InternalCompactRecord record = (InternalCompactRecord) iterator.next();
                    if (!record.hasField(fieldName)) {
                        iterator.remove();
                        multiResult.setNullOrEmptyTarget(true);
                        continue;
                    }
                    InternalCompactRecord subCompactRecord = (InternalCompactRecord) record.getCompactRecord(fieldName);
                    if (subCompactRecord == null) {
                        iterator.remove();
                        multiResult.setNullOrEmptyTarget(true);
                        continue;
                    }
                    iterator.set(subCompactRecord);
                }
            } else if (path.endsWith("[any]")) {
                // ex: attribute any
                while (iterator.hasNext()) {
                    InternalCompactRecord record = (InternalCompactRecord) iterator.next();
                    iterator.remove();
                    if (!record.hasField(fieldName)) {
                        multiResult.setNullOrEmptyTarget(true);
                        continue;
                    }
                    CompactRecord[] compactRecords = record.getCompactRecordArray(fieldName);
                    if (compactRecords == null || compactRecords.length == 0) {
                        multiResult.setNullOrEmptyTarget(true);
                        continue;
                    }
                    for (CompactRecord compactRecord : compactRecords) {
                        if (compactRecord != null) {
                            iterator.add(compactRecord);
                        } else {
                            multiResult.setNullOrEmptyTarget(true);
                        }
                    }
                }
            } else {
                // ex: attribute[2]
                int index = Integer.parseInt(extractArgumentsFromAttributeName(path));
                while (iterator.hasNext()) {
                    InternalCompactRecord record = (InternalCompactRecord) iterator.next();
                    if (!record.hasField(fieldName)) {
                        iterator.remove();
                        multiResult.setNullOrEmptyTarget(true);
                        continue;
                    }
                    CompactRecord compactRecord = record.getCompactRecordFromArray(fieldName, index);
                    if (compactRecord != null) {
                        iterator.set(compactRecord);
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
                InternalCompactRecord record = (InternalCompactRecord) iterator.next();
                Object leaf = readLeaf(record, fieldName);
                iterator.set(leaf);
            }
        } else if (path.endsWith("[any]")) {
            // ex: attribute any
            while (iterator.hasNext()) {
                InternalCompactRecord record = (InternalCompactRecord) iterator.next();
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
                CompactRecord record = (CompactRecord) iterator.next();
                Object leaf = readIndexed((InternalCompactRecord) record, fieldName, index);
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

    private Object readIndexed(InternalCompactRecord record, String path, int index) {
        if (!record.hasField(path)) {
            return null;
        }
        TypeID type = record.getFieldType(path);
        return fieldOperations(type).readIndexed(record, path, index);
    }

    private Object readLeaf(InternalCompactRecord record, String path) {
        if (!record.hasField(path)) {
            return null;
        }
        TypeID type = record.getFieldType(path);
        return fieldOperations(type).readObject(record, path);
    }

}
