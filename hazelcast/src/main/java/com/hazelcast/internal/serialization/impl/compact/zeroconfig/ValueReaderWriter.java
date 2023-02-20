/*
 * Copyright (c) 2008-2023, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.internal.serialization.impl.compact.zeroconfig;

import com.hazelcast.internal.serialization.impl.compact.Schema;
import com.hazelcast.nio.serialization.compact.CompactReader;
import com.hazelcast.nio.serialization.compact.CompactWriter;

/**
 * Subclasses of this class are expected to read or write values directly from
 * the Compact reader and writers, using the provided field name.
 * <p>
 * It is expected that the Reflective and Record serializers will re-use those
 * subclasses to avoid code duplication.
 *
 * @param <T> Type of the value to read/write
 */
public abstract class ValueReaderWriter<T> {

    protected final String fieldName;

    protected ValueReaderWriter(String fieldName) {
        this.fieldName = fieldName;
    }

    public abstract T read(CompactReader reader, Schema schema);

    public abstract void write(CompactWriter writer, T value);
}
