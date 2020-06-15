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

import com.hazelcast.nio.serialization.FieldType;
import com.hazelcast.nio.serialization.PortableReader;
import com.hazelcast.query.impl.getters.Extractors;
import com.hazelcast.sql.impl.QueryException;
import com.hazelcast.sql.impl.type.QueryDataType;

import java.io.IOException;

/**
 * An extractor that uses {@link com.hazelcast.query.impl.getters.Extractors} for field retrieval.
 */
public class GenericFieldExtractor extends AbstractGenericExtractor {

    private final Extractors extractors;
    private final String path;

    public GenericFieldExtractor(
        boolean key,
        GenericTargetAccessor targetAccessor,
        QueryDataType type,
        Extractors extractors,
        String path
    ) {
        super(key, targetAccessor, type);

        this.extractors = extractors;
        this.path = path;
    }

    @Override
    public Object get() {
        try {
            // TODO: PortableExtractor ?
            Object target = getTarget();
            if (target instanceof PortableReader) {
                PortableReader reader = (PortableReader) getTarget();
                return type.convert(extract(reader, path));
            } else {
                return type.convert(extractors.extract(target, path, null));
            }
        } catch (Exception e) {
            throw QueryException.dataException("Cannot extract " + (key ? "key" : "value") + " field \"" + path
                    + "\" of type " + type + ": " + e.getMessage(), e);
        }
    }

    private static Object extract(PortableReader reader, String path) throws IOException {
        if (!reader.hasField(path)) {
            return null;
        }

        FieldType portableType = reader.getFieldType(path);
        switch (portableType) {
            case BOOLEAN:
                return reader.readBoolean(path);
            case BYTE:
                return reader.readByte(path);
            case SHORT:
                return reader.readShort(path);
            case CHAR:
                return reader.readChar(path);
            case UTF:
                return reader.readUTF(path);
            case INT:
                return reader.readInt(path);
            case LONG:
                return reader.readLong(path);
            case FLOAT:
                return reader.readFloat(path);
            case DOUBLE:
                return reader.readDouble(path);
            default:
                return reader.readPortable(path);
        }
    }
}
