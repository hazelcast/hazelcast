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

import javax.annotation.Nullable;
import java.util.List;
import java.util.Objects;
import java.util.UUID;

public class IterationResult<T> {
    private final List<T> page;
    private final UUID cursorId;
    private final UUID cursorIdToForget;

    public IterationResult(List<T> page, UUID cursorId, @Nullable UUID cursorIdToForget) {
        this.page = page;
        this.cursorId = cursorId;
        this.cursorIdToForget = cursorIdToForget;
    }

    public List<T> getPage() {
        return page;
    }

    public UUID getCursorId() {
        return cursorId;
    }

    public UUID getCursorIdToForget() {
        return cursorIdToForget;
    }

    public boolean isEmpty() {
        return page.isEmpty();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        IterationResult<?> that = (IterationResult<?>) o;
        return Objects.equals(page, that.page) && Objects.equals(cursorId, that.cursorId);
    }

    @Override
    public int hashCode() {
        return Objects.hash(page, cursorId);
    }
}
