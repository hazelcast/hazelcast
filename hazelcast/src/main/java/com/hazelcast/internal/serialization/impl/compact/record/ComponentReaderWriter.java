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

package com.hazelcast.internal.serialization.impl.compact.record;

import com.hazelcast.internal.serialization.impl.compact.Schema;
import com.hazelcast.nio.serialization.compact.CompactReader;
import com.hazelcast.nio.serialization.compact.CompactWriter;

/**
 * Reads or writes a single component of the record object to reader/writer.
 */
public interface ComponentReaderWriter {

    /**
     * @param compactReader to read the component from.
     * @param schema to validate expected and actual types.
     * @return a single component of the record object.
     */
    Object readComponent(CompactReader compactReader, Schema schema);

    /**
     * @param compactWriter to write the component to.
     * @param recordObject to get the component from.
     * @throws Exception in case the reflective access to the component fails.
     */
    void writeComponent(CompactWriter compactWriter, Object recordObject) throws Exception;
}
