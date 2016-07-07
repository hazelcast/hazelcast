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

package com.hazelcast.jet.memory.spilling;

import com.hazelcast.jet.memory.Partition;
import com.hazelcast.nio.Disposable;

/**
 * Spills, merges, and reads data from memory to disk and back.
 */
public interface Spiller extends Disposable {
    /** Starts the spilling process for a set of partitions stored in memory block */
    void start(Partition[] source);

    /**
     * Reads the current spill-file and merges it with in-memory storage.
     * @return {@code true} if there is more data to merge, {@code false} if all done
     */
    boolean processNextChunk();

    /** @return the reader object which reads spilled data from file */
    SpillFileCursor openSpillFileCursor();

    /** Stops the spilling process: close all opened resources and perform all finalization steps. */
    void stop();
}
