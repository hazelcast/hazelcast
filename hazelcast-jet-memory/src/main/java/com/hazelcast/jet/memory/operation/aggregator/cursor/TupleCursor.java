/*
 * Copyright (c) 2008-2016, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.jet.memory.operation.aggregator.cursor;

import com.hazelcast.jet.io.tuple.Tuple2;
import com.hazelcast.jet.memory.binarystorage.comparator.Comparator;

/**
 * Cursor over a range of tuples.
 */
public interface TupleCursor {
    void reset(Comparator comparator);

    boolean advance();

    /**
     * Returns the current cursor state as a tuple. May return itself, therefore
     * the returned object is valid only until {@code advance} or {@code reset} is
     * called on this cursor.
     */
    Tuple2 asTuple();
}
