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
import java.util.Collection;
import java.util.Map.Entry;

import static com.hazelcast.jet.impl.util.LoggingUtil.logFine;
import static com.hazelcast.jet.impl.util.LoggingUtil.logFinest;
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
    public static final String SNAPSHOT_DATA_NAME_PREFIX = SNAPSHOT_NAME_PREFIX + "data.";

    // key for the entry that points to the latest snapshot
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
    long registerSnapshot(long jobId, Collection<String> vertexNames) {
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
        return instance.getMap(snapshotsMapName(jobId));
    }

    private MaxByAggregator<Entry<Long, Object>> maxByAggregator() {
        return new MaxByAggregator<>("snapshotId");
    }

    public static String snapshotsMapName(long jobId) {
        return SNAPSHOT_NAME_PREFIX + idToString(jobId);
    }

    public static String snapshotDataMapName(long jobId, long snapshotId, String vertexName) {
        return SNAPSHOT_DATA_NAME_PREFIX + idToString(jobId) + '.' + snapshotId + '.' + vertexName;
    }

    /**
     * Deletes snapshot data and records from snapshotsMap except one.
     * <p>
     * Method must be run when there's no ongoing snapshot, because it also
     * deletes the ongoing snapshots. If we omitted them, then interrupted
     * ongoing snapshots will never be deleted.
     *
     * @param snapshotToKeep the current snapshot to keep
     */
    void deleteAllSnapshotsExceptOne(long jobId, Long snapshotToKeep) {
        final IStreamMap<Long, SnapshotRecord> snapshotMap = getSnapshotMap(jobId);
        Predicate<Long, SnapshotRecord> predicate =
                e -> !e.getKey().equals(LATEST_STARTED_SNAPSHOT_ID_KEY) && !e.getKey().equals(snapshotToKeep);

        for (Entry<Long, SnapshotRecord> entry : snapshotMap.entrySet(predicate)) {
            deleteSnapshot(snapshotMap, entry.getValue());
        }
    }

    /**
     * Delete a single snapshot for a given job if it exists
     */
    void deleteSingleSnapshot(long jobId, Long snapshotId) {
        final IStreamMap<Long, SnapshotRecord> snapshotMap = getSnapshotMap(jobId);
        SnapshotRecord record = snapshotMap.get(snapshotId);
        if (record != null) {
            deleteSnapshot(snapshotMap, record);
        }
    }

    /**
     * Delete all snapshots for a given job
     */
    void deleteAllSnapshots(long jobId) {
        final IStreamMap<Long, SnapshotRecord> snapshotMap = getSnapshotMap(jobId);
        Predicate predicate = e -> !e.getKey().equals(LATEST_STARTED_SNAPSHOT_ID_KEY);
        for (Entry<Long, SnapshotRecord> entry : snapshotMap.entrySet(predicate)) {
            deleteSnapshotData(entry.getValue());
        }
        logFine(logger, "Deleted all snapshots for job %s", idToString(jobId));
        snapshotMap.destroy();
    }

    private void deleteSnapshot(IStreamMap<Long, SnapshotRecord> map, SnapshotRecord record) {
        setSnapshotStatus(record.jobId(), record.snapshotId(), SnapshotStatus.TO_DELETE);
        deleteSnapshotData(record);
        map.remove(record.snapshotId());
        logFinest(logger, "Deleted snapshot record for snapshot %d for job %s",
                record.snapshotId(), idToString(record.jobId()));
    }

    private void deleteSnapshotData(SnapshotRecord record) {
        for (String vertexName : record.vertices()) {
            String mapName = snapshotDataMapName(record.jobId(), record.snapshotId(), vertexName);
            instance.getMap(mapName).destroy();
            logFine(logger, "Deleted snapshot data for snapshot %d for job %s and vertex '%s'",
                    record.snapshotId(), idToString(record.jobId()), vertexName);
        }
    }
}
