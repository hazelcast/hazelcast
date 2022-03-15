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

package com.hazelcast.jet.impl.execution;

import java.util.Objects;

/**
 * Special item interleaved with other items on queue to signal a start of a
 * snapshot.
 */
public final class SnapshotBarrier implements BroadcastItem {
    private final long snapshotId;
    private final boolean isTerminal;

    public SnapshotBarrier(long snapshotId, boolean isTerminal) {
        assert snapshotId >= 0; // snapshot ID starts at 0 and is only incremented
        this.snapshotId = snapshotId;
        this.isTerminal = isTerminal;
    }

    public long snapshotId() {
        return snapshotId;
    }

    public boolean isTerminal() {
        return isTerminal;
    }

    @Override
    public String toString() {
        return "SnapshotBarrier{snapshotId=" + snapshotId + (isTerminal ? ", terminal" : "") + '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        SnapshotBarrier that = (SnapshotBarrier) o;
        return snapshotId == that.snapshotId &&
                isTerminal == that.isTerminal;
    }

    @Override
    public int hashCode() {
        return Objects.hash(snapshotId, isTerminal);
    }
}
