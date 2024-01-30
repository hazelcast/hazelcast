/*
 * Copyright (c) 2008-2024, Hazelcast, Inc. All Rights Reserved.
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

import com.hazelcast.internal.util.UuidUtil;

import javax.annotation.Nonnull;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.UUID;

import static java.util.Objects.requireNonNull;

public class IteratorWithCursor<T> {
    // iterator to paginate
    private final Iterator<T> iterator;
    // represent the current position in the collection
    private UUID prevCursorId;
    private UUID cursorId;
    private long lastAccessTime;
    private List<T> page;

    public IteratorWithCursor(Iterator<T> iterator, UUID cursorId) {
        this.iterator = iterator;
        this.cursorId = cursorId;
        this.lastAccessTime = System.currentTimeMillis();
    }

    /**
     * Returns the next {@link IterationResult} using the iterator, or the previous page if the cursor id matches the
     * previous cursor id. The cursor id is updated to a new value if the current cursor id is received.
     * <p>
     * @param cursorId cursor represents the last seen item by the caller. it has to match the cursor of the paginator or the
     *                 previous cursor returned by the paginator. Must not be null.
     * @param maxCount the maximum number of items to return
     */
    public IterationResult<T> iterate(@Nonnull UUID cursorId, int maxCount) {
        requireNonNull(cursorId);
        if (cursorId.equals(this.prevCursorId)) {
            access();
            // no progress, no need to forget a cursor id, so null
            return new IterationResult<>(this.page, this.cursorId, null);
        } else if (!cursorId.equals(this.cursorId)) {
            throw new IllegalStateException("The cursor id " + cursorId
                    + " is not the current cursor id nor the previous cursor id.");
        }
        List<T> currentPage = new ArrayList<>(maxCount);
        while (currentPage.size() < maxCount && iterator.hasNext()) {
            currentPage.add(iterator.next());
        }
        UUID cursorIdToForget = this.prevCursorId;
        this.prevCursorId = this.cursorId;
        this.cursorId = UuidUtil.newUnsecureUUID();
        this.page = currentPage;
        access();
        return new IterationResult<>(this.page, this.cursorId, cursorIdToForget);
    }

    public long getLastAccessTime() {
        return lastAccessTime;
    }

    private void access() {
        this.lastAccessTime = System.currentTimeMillis();
    }
}
