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

package com.hazelcast.sql.impl;

import java.util.Iterator;

/**
 * Java standard {@link Iterator} enhanced with {@link
 * #hasNextImmediately()} to allow for non-blocking iteration.
 */
public interface ResultIterator<T> extends Iterator<T> {

    /**
     * Checks if a next item is available immediately.
     * <p>
     * The implementation is free to either:<ul>
     *     <li>block until an item is available or the end is reached
     *     <li>not block and return {@link HasNextImmediatelyResult#RETRY} if we're
     *         not done and an item is not immediately available.
     * </ul>
     * <p>
     * This can be used to implement either blocking or non-blocking behavior.
     *
     * @return see {@link HasNextImmediatelyResult}
     */
    HasNextImmediatelyResult hasNextImmediately();

    enum HasNextImmediatelyResult {
        /**
         * The next item is available immediately. Subsequent {@link #next()} call
         * is guaranteed to succeed and not block.
         */
        YES,

        /**
         * Another item is not available immediately, but might be available later.
         * The caller should check again later. Also there might not be a next
         * item at all.
         */
        RETRY,

        /**
         * The last item was already returned. A call to {@link #next()} will fail.
         * The iterator is exhausted.
         */
        DONE
    }
}
