/*
 * Copyright (c) 2008-2026, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.cp.internal.raft;

import com.hazelcast.cp.CPGroupId;
import com.hazelcast.cp.internal.datastructures.snapshot.DataChunkGroup;

import javax.annotation.Nonnull;
import java.util.Iterator;

/**
 * An extension of the {@link SnapshotAwareService} interface that
 * supports chunked snapshotting. This interface allows for the
 * creation and restoration of snapshots in smaller, manageable chunks,
 * which is particularly useful for large datasets where transferring
 * or processing the entire snapshot at once may be impractical.
 *
 * @param <C> The type of data contained in each chunk of the snapshot.
 * @param <T> The type of data managed by the {@link SnapshotAwareService}.
 * @see SnapshotAwareService
 * @see com.hazelcast.cp.internal.datastructures.snapshot.ChunkUtil#PROP_MAX_CHUNK_SIZE_IN_MB
 */
public interface ChunkedSnapshotAwareService<C, T> extends SnapshotAwareService<T> {

    /**
     * Creates a chunked snapshot for the specified {@link CPGroupId} and commit index.
     * This method generates an iterator over the chunks of the snapshot, allowing for
     * incremental processing or transfer of the snapshot data.
     *
     * @param groupId     The {@link CPGroupId} for which the snapshot is being created.
     * @param commitIndex The commit index associated with the snapshot.
     * @return an iterator over the generated chunks of the snapshot.
     */
    Iterator<DataChunkGroup<C>> takeSnapshotChunks(@Nonnull CPGroupId groupId, long commitIndex);

    /**
     * Restores a single chunk of the snapshot for the specified {@link CPGroupId} and commit index.
     * This method is used to incrementally restore a snapshot by processing one chunk at a time.
     *
     * @param groupId     The {@link CPGroupId} for which the snapshot is being restored.
     * @param commitIndex The commit index associated with the snapshot being restored.
     * @param dataChunk   The chunk of snapshot data to be restored.
     * @throws NullPointerException If the provided chunk is null.
     */
    void restoreSnapshotChunk(@Nonnull CPGroupId groupId,
                              long commitIndex, @Nonnull DataChunkGroup<C> dataChunk);

    /**
     * Resets the service state in preparation for restoring a chunked snapshot.
     * This method ensures that the service is in a clean state before applying
     * the snapshot chunks.
     *
     * @param groupId The {@link CPGroupId} for which the snapshot restoration is being prepared.
     * @throws NullPointerException If the provided groupId is null.
     */
    void prepareForSnapshotRestore(@Nonnull CPGroupId groupId);

    @Override
    default void restoreSnapshot(CPGroupId groupId, long commitIndex, T snapshot) {
        throw new UnsupportedOperationException(
                "Chunked services are restored per chunk via restoreSnapshotChunk()");
    }
}
