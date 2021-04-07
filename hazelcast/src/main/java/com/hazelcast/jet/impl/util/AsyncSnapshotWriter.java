/*
 * Copyright (c) 2008-2021, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.jet.impl.util;

import com.hazelcast.internal.serialization.Data;

import javax.annotation.CheckReturnValue;
import java.util.Map.Entry;

public interface AsyncSnapshotWriter {
    @CheckReturnValue
    boolean offer(Entry<? extends Data, ? extends Data> entry);

    @CheckReturnValue
    boolean flushAndResetMap();
    void resetStats();

    boolean hasPendingAsyncOps();

    /**
     * @return any error occurred during writing to underlying map. Error is
     * reported only once, next call will return {@code null} unless another
     * error happens.
     */
    Throwable getError();

    boolean isEmpty();

    long getTotalPayloadBytes();
    long getTotalKeys();
    long getTotalChunks();
}
