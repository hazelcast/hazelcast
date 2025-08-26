/*
 * Copyright (c) 2008-2025, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.internal.iteration;

import com.hazelcast.spi.impl.NodeEngine;

import java.util.Iterator;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;


/**
 * This class manages the replicated map iterations that have pagination support.
 * <p>
 * This class's methods for creating an iterator ({@link IteratorWithCursorManager#createIterator(Iterator, UUID)}),
 * progressing an iterator {@link IteratorWithCursorManager#iterate(UUID, int)} and cleaning up an iterator accessed concurrently
 * All replicated map's iterators are managed via this class.
 * @param <T>
 */
public class IteratorWithCursorManager<T> {

    private final NodeEngine nodeEngine;
    private final ConcurrentHashMap<UUID, IteratorWithCursor<T>> iterators = new ConcurrentHashMap<>();
    // Used to keep track of the iterator id for a cursor id. This makes the client protocol simpler, not needing another UUID
    // for the iterator id to be sent for each request.
    private final ConcurrentHashMap<UUID, UUID> cursorToIteratorId = new ConcurrentHashMap<>();

    public IteratorWithCursorManager(NodeEngine nodeEngine) {
        this.nodeEngine = nodeEngine;
    }

    /**
     * Creates a new iterator with the cursor id if it already does not exist.
     * @param items the items to iterate over
     * @param iteratorId id of the iterator
     */
    public void createIterator(Iterator<T> items, UUID iteratorId) {
        if (!iterators.containsKey(iteratorId)) {
            iterators.put(iteratorId, new IteratorWithCursor<>(items, iteratorId));
            cursorToIteratorId.put(iteratorId, iteratorId);
        }
    }

    public IterationResult<T> iterate(UUID cursorId, int maxCount) {
        UUID iteratorId = cursorToIteratorId.get(cursorId);
        if (iteratorId == null) {
            throw new IllegalStateException("There is no iteration with cursor id " + cursorId + " on member "
                    + this.nodeEngine.getThisAddress() + ".");
        }
        IteratorWithCursor<T> paginator = iterators.get(iteratorId);
        if (paginator == null) {
            throw new IllegalStateException("There is no iteration with cursor id " + cursorId + " on member "
                    + this.nodeEngine.getThisAddress() + ".");
        }
        IterationResult<T> result = paginator.iterate(cursorId, maxCount);
        if (result.getCursorIdToForget() != null) {
            // Remove the previous cursor id.
            cursorToIteratorId.remove(result.getCursorIdToForget());
        }
        // Put the new cursor id.
        cursorToIteratorId.put(result.getCursorId(), iteratorId);
        return result;
    }

    public void cleanupIterator(UUID iteratorId) {
        if (!iterators.containsKey(iteratorId)) {
            // silently ignore the nonexistent iterator cleanup request.
            return;
        }
        iterators.remove(iteratorId);
        cursorToIteratorId.values().remove(iteratorId);
    }

    ConcurrentHashMap<UUID, UUID> getCursorToIteratorId() {
        return cursorToIteratorId;
    }

    public ConcurrentHashMap<UUID, IteratorWithCursor<T>> getIterators() {
        return iterators;
    }

    public ConcurrentHashMap.KeySetView<UUID, IteratorWithCursor<T>> getKeySet() {
        return iterators.keySet();
    }

    public IteratorWithCursor<T> getIterator(UUID iteratorId) {
        return iterators.get(iteratorId);
    }
}
