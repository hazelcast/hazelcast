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

package com.hazelcast.sql.impl.extract;

import com.hazelcast.internal.serialization.Data;
import com.hazelcast.internal.serialization.InternalSerializationService;
import com.hazelcast.internal.serialization.impl.DefaultPortableReader;
import com.hazelcast.sql.impl.QueryException;
import com.hazelcast.sql.impl.type.QueryDataType;

public class PortableQueryTarget implements QueryTarget {

    private final int factoryId;
    private final int classId;
    private final InternalSerializationService serializationService;
    private final boolean isKey;

    private Data target;
    private DefaultPortableReader reader;

    public PortableQueryTarget(
        int factoryId,
        int classId,
        InternalSerializationService serializationService,
        boolean isKey
    ) {
        this.factoryId = factoryId;
        this.classId = classId;
        this.serializationService = serializationService;
        this.isKey = isKey;
    }

    @Override
    public void setTarget(Object target) {
        try {
            if (target instanceof Data) {
                Data target0 = (Data) target;

                if (target0.isPortable()) {
                    DefaultPortableReader reader = (DefaultPortableReader) serializationService.createPortableReader(target0);

                    if (factoryId != reader.getFactoryId()) {
                        throw QueryException.dataException("Unexpected portable class factory ID ["
                            + "expected=" + factoryId + ", actual=" + reader.getFactoryId() + ']');
                    }

                    if (classId != reader.getClassId()) {
                        throw QueryException.dataException("Unexpected portable class ID ["
                            + "expected=" + classId + ", actual=" + reader.getClassId() + ']');
                    }

                    this.target = target0;
                    this.reader = reader;
                } else {
                    throw QueryException.dataException("Object is not Portable");
                }
            } else {
                throw QueryException.dataException("Object is not Portable");
            }
        } catch (QueryException e) {
            throw e;
        } catch (Exception e) {
            throw QueryException.dataException(e.getMessage(), e);
        }
    }

    @Override
    public QueryExtractor createExtractor(String path, QueryDataType type) {
        if (path == null) {
            return new TargetExtractor(type);
        } else {
            return new FieldExtractor(type, path);
        }
    }

    private Object getTarget(QueryDataType type) {
        try {
            return type.convert(serializationService.toObject(target));
        } catch (QueryException e) {
            throw e;
        } catch (Exception e) {
            throw QueryException.dataException("Cannot convert object to " + type, e);
        }
    }

    private Object getField(String path, QueryDataType type) {
        try {
            return type.convert(reader.read(path));
        } catch (QueryException e) {
            throw e;
        } catch (Exception e) {
            throw QueryException.dataException("Cannot convert object field \"" + path + "\" to " + type, e);
        }
    }

    private final class TargetExtractor implements QueryExtractor {

        private final QueryDataType type;

        private TargetExtractor(QueryDataType type) {
            this.type = type;
        }

        @Override
        public Object get() {
            return getTarget(type);
        }
    }

    private final class FieldExtractor implements QueryExtractor {

        private final QueryDataType type;
        private final String path;

        private FieldExtractor(QueryDataType type, String path) {
            this.type = type;
            this.path = path;
        }

        @Override
        public Object get() {
            return getField(path, type);
        }
    }
}
