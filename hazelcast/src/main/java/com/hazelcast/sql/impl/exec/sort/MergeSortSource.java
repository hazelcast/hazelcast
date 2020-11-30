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

package com.hazelcast.sql.impl.exec.sort;

import com.hazelcast.sql.impl.row.Row;

/**
 * Source for merge-sort values.
 */
public interface MergeSortSource {
    /**
     * Try advancing the source.
     *
     * @return {@code true} if advanced successfully, {@code false} in data is available at the moment. In the latter case
     * {@link #isDone()} method should be called to distinguish between missing data and finished input stream.
     */
    boolean advance();

    /**
     * @return {@code true} if the source is done, i.e. no more data is expected, {@code false} otherwise.
     */
    boolean isDone();

    /**
     * @return Current sort key.
     */
    SortKey peekKey();

    /**
     * @return Current row.
     */
    Row peekRow();
}
