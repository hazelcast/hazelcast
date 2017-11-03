/*
 * Copyright (c) 2008-2017, Hazelcast, Inc. All Rights Reserved.
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
 */
public enum IndexCopyBehavior {
    /**
     * READ - Internal data structures of the index are concurrently modified without copy-on-write semantics.
     * Index queries copy the results of a query on index read to detach the result from the source map.
     * Should be used in index-write intensive cases. (default value).
     */
    READ,
    /**
     * WRITE - Internal data structures of the index are modified with copy-on-write semantics.
     * Previously returned indexed query results reflect the state of the index at the time of the query and are not
     * affected by future index modifications.
     */
    WRITE,
    /**
     * NEVER - Internal data structures of the index are concurrently modified without copy-on-write semantics.
     * Index reads never copy the results of a query to a separate map.
     * It means that the results backed by the underlying index-map can change after the query has been executed.
     * Specifically an entry might have been added / removed from an index, or it might have been remapped.
     */
    NEVER
}
