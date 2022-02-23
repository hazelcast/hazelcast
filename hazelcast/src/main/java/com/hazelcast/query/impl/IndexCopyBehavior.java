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

package com.hazelcast.query.impl;

/**
 * Defines the behavior for index copying on index read/write.
 *
 * Supported in BINARY and OBJECT in-memory-formats. Ignored in NATIVE in-memory-format.
 *
 * Why is it needed? In order to support correctness the internal data-structures used by indexes need to do some copying.
 * The copying may take place on-read or on-write:
 * <p>
 * -&gt; Copying on-read means that each index-read operation will copy the result of the query before returning it to the caller.
 * This copying may be expensive, depending on the size of the result, since the result is stored in a map, which means
 * that all entries need to have the hash calculated before being stored in a bucket.
 * Each index-write operation however will be fast, since there will be no copying taking place.
 * <p>
 * -&gt; Copying on-write means that each index-write operation will completely copy the underlying map to provide the
 * copy-on-write semantics. Depending on the index size, it may be a very expensive operation.
 * Each index-read operation will be very fast, however, since it may just access the map and return it to the caller.
 * <p>
 * -&gt; Never copying is tricky. It means that the internal data structures of the index are concurrently modified without
 * copy-on-write semantics. Index reads never copy the results of a query to a separate map.
 * It means that the results backed by the underlying index-map can change after the query has been executed.
 * Specifically an entry might have been added / removed from an index, or it might have been remapped.
 * Should be used in cases when a the caller expects "mostly correct" results - specifically, if it's ok
 * if some entries returned in the result set do not match the initial query criteria.
 * The fastest solution for read and writes, since no copying takes place.
 * <p>
 * It's a tuneable trade-off - the user may decide.
 */
public enum IndexCopyBehavior {
    /**
     * Internal data structures of the index are concurrently modified without copy-on-write semantics.
     * Index queries copy the results of a query on index read to detach the result from the source map.
     * Should be used in index-write intensive cases, since the reads will slow down due to the copying.
     * Default value.
     */
    COPY_ON_READ,
    /**
     * Internal data structures of the index are modified with copy-on-write semantics.
     * Previously returned index query results reflect the state of the index at the time of the query and are not
     * affected by future index modifications.
     * Should be used in index-read intensive cases, since the writes will slow down due to the copying.
     */
    COPY_ON_WRITE,
    /**
     * Internal data structures of the index are concurrently modified without copy-on-write semantics.
     * Index reads never copy the results of a query to a separate map.
     * It means that the results backed by the underlying index-map can change after the query has been executed.
     * Specifically an entry might have been added / removed from an index, or it might have been remapped.
     * Should be used in cases when a the caller expects "mostly correct" results - specifically, if it's ok
     * if some entries returned in the result set do not match the initial query criteria.
     * The fastest solution for read and writes, since no copying takes place.
     */
    NEVER
}
