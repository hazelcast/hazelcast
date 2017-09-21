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

package com.hazelcast.jet.impl;

import com.hazelcast.aggregation.impl.MaxByAggregator;
import com.hazelcast.core.IMap;
import com.hazelcast.jet.JetInstance;
import com.hazelcast.jet.impl.execution.SnapshotRecord;
import com.hazelcast.jet.impl.execution.SnapshotRecord.SnapshotStatus;
import com.hazelcast.jet.stream.IStreamMap;
import com.hazelcast.logging.ILogger;
import com.hazelcast.query.Predicate;

import javax.annotation.Nullable;
import java.util.Map.Entry;
import java.util.Set;

import static com.hazelcast.jet.impl.util.Util.compute;
import static com.hazelcast.jet.impl.util.Util.idToString;

public class SnapshotRepository {

    /**
     * Name of internal IMaps which store snapshot related data.
     * <p>
     * Snapshot metadata is stored in the following map:
     * <pre>SNAPSHOT_NAME_PREFIX + jobId</pre>
     * <p>
     * Snapshot data for one vertex is stored in the following map:
     * <pre>SNAPSHOT_NAME_PREFIX + jobId + '.' + snapshotId + '.' + vertexName</pre>
     */
    public static final String SNAPSHOT_NAME_PREFIX = "__jet.snapshots.";

    private static final long LATEST_STARTED_SNAPSHOT_ID_KEY = -1;

    private final JetInstance instance;
    private final ILogger logger;

    public SnapshotRepository(JetInstance jetInstance) {
        this.instance = jetInstance;
        this.logger = jetInstance.getHazelcastInstance().getLoggingService().getLogger(getClass());
    }

    /**
     * Registers a new snapshot. Returns the ID for the registered snapshot
     */
    long registerSnapshot(long jobId, Set<String> vertexNames) {
        IStreamMap<Long, Object> snapshots = getSnapshotMap(jobId);

        SnapshotRecord record;
        do {
            long nextSnapshotId = generateNextSnapshotId(snapshots);
            record = new SnapshotRecord(jobId, nextSnapshotId, vertexNames);
        } while (snapshots.putIfAbsent(record.snapshotId(), record) != null);
        return record.snapshotId();
    }

    private long generateNextSnapshotId(IStreamMap<Long, Object> snapshots) {
        Long snapshotId;
        long nextSnapshotId;
        do {
            snapshotId = (Long) snapshots.get(LATEST_STARTED_SNAPSHOT_ID_KEY);
            nextSnapshotId = (snapshotId == null) ? 0 : (snapshotId + 1);
        } while (!replaceAllowingNull(snapshots, LATEST_STARTED_SNAPSHOT_ID_KEY, snapshotId, nextSnapshotId));

        return nextSnapshotId;
    }

    /**
     * Alternative for {@link IMap#replace(Object, Object, Object)} allowing null for {@code oldValue}.
     */
    private static <K, V> boolean replaceAllowingNull(IMap<K, V> map, K key, V oldValue, V newValue) {
        if (oldValue == null) {
            return map.putIfAbsent(key, newValue) == null;
        } else {
            return map.replace(key, oldValue, newValue);
        }
    }

    /**
     * Updates status of the given snapshot. Returns the elapsed time for the snapshot.
     */
    long setSnapshotStatus(long jobId, long snapshotId, SnapshotStatus status) {
        IStreamMap<Long, SnapshotRecord> snapshots = getSnapshotMap(jobId);
        SnapshotRecord record = compute(snapshots, snapshotId, (k, r) -> {
            r.setStatus(status);
            return r;
        });
        return System.currentTimeMillis() - record.startTime();
    }

    /**
     * Return the newest complete snapshot ID for the specified job or null if no such snapshot is found.
     */
    @Nullable
    Long latestCompleteSnapshot(long jobId) {
        IStreamMap<Long, Object> snapshotMap = getSnapshotMap(jobId);
        MaxByAggregator<Entry<Long, Object>> entryMaxByAggregator = maxByAggregator();
        Predicate<Long, Object> completedSnapshots = (Predicate<Long, Object>) e -> {
            Object value = e.getValue();
            return value instanceof SnapshotRecord && ((SnapshotRecord) value).isSuccessful();
        };
        Entry<Long, Object> entry = snapshotMap.aggregate(entryMaxByAggregator, completedSnapshots);
        return entry != null ? entry.getKey() : null;
    }

    /**
     * Return the latest started snapshot ID for the specified job or null if no such snapshot is found.
     */
    @Nullable
    Long latestStartedSnapshot(long jobId) {
        IMap<Long, Long> map = getSnapshotMap(jobId);
        return map.get(LATEST_STARTED_SNAPSHOT_ID_KEY);
    }

    public <T> IStreamMap<Long, T> getSnapshotMap(long jobId) {
        return instance.getMap(SNAPSHOT_NAME_PREFIX + idToString(jobId));
    }

    private MaxByAggregator<Entry<Long, Object>> maxByAggregator() {
        return new MaxByAggregator<>("snapshotId");
    }

    public static String snapshotDataMapName(long jobId, long snapshotId, String vertexName) {
        return SNAPSHOT_NAME_PREFIX + idToString(jobId) + '.' + snapshotId + '.' + vertexName;
    }

    /**
     * Deletes snapshot data and records from snapshotsMap for single job.
     * <p>
     * Method must be run when there's no ongoing snapshot, because it also
     * deletes the ongoing snapshots. If we omitted them, then interrupted
     * snapshots will never be deleted.
     *
     * @param validatedSnapshotToKeep If not null, the snapshot with supplied ID will not be deleted. If null,
     *                               all snapshots including the snapshotMap itself will be deleted
     */
    void deleteSnapshots(long jobId, Long validatedSnapshotToKeep) {
        final IStreamMap<Long, SnapshotRecord> snapshotMap = getSnapshotMap(jobId);

        Predicate<Long, SnapshotRecord> predicate =
                e -> !e.getKey().equals(LATEST_STARTED_SNAPSHOT_ID_KEY) && !e.getKey().equals(validatedSnapshotToKeep);

        for (Entry<Long, SnapshotRecord> entry : snapshotMap.entrySet(predicate)) {
            long snapshotId = entry.getValue().snapshotId();
            // set the status so that it can't be used if the deletion fails
            setSnapshotStatus(jobId, snapshotId, SnapshotStatus.TO_DELETE);
            for (String vertexName : entry.getValue().vertices()) {
                String mapName = snapshotDataMapName(jobId, snapshotId, vertexName);
                instance.getHazelcastInstance().getMap(mapName).destroy();
            }
            if (validatedSnapshotToKeep != null) {
                // This is optimized to most-common case, when we delete just one old snapshot: map.remove()
                // sends operation to just one node, map.removeAll() would send one to all nodes.
                snapshotMap.remove(entry.getKey());
            }
            logger.info("Deleted snapshot " + snapshotId + " for " + idToString(jobId));
        }

        if (validatedSnapshotToKeep == null) {
            snapshotMap.destroy();
        }
    }
}
