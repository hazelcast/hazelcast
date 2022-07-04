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

package com.hazelcast.sql.impl.exec.scan;

import com.hazelcast.internal.serialization.Data;

/**
 * Iterator over key/value pairs.
 */
public interface KeyValueIterator {
    /**
     * Advances the iterator to the next available record.
     * <p>
     * If the method has returned {@code true}, the key and the value could be accessed through
     * {@link #getKey()} and {@link #getValue()} respectively.
     *
     * @return {@code true} if the next record is available, {@code false} if no more records are available
     */
    boolean tryAdvance();

    /**
     *
     * @return {@code true} if there are no more entries
     */
    boolean done();

    Object getKey();
    Data getKeyData();
    Object getValue();
    Data getValueData();
}
