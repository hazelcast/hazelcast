/*
 * Copyright (c) 2008-2018, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.map.impl;

import com.hazelcast.core.IMap;
import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;
import com.hazelcast.logging.LoggingService;

import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;

/**
 * Class tracking the progress of map record loading. It logs the
 * progress in every 10 seconds if there is loading activity, which
 * triggers logging.
 * <p>
 * The tracking is done in separate instances for each map, which are
 * shared between partitions on the same node. The instances are created
 * on the first access of the map, and are destroyed with destroying the
 * map they belong to.
 * <p>
 * <b>Notes:</b>
 * <ul>
 * <li>Only batch loading is tracked by this class, loading individual
 * keys are not.
 * <li>This class logs the number of records loaded in the last 10
 * seconds but does not log when the load starts or completes. This is
 * because the current map loader implementation does not provide
 * callbacks for these events.
 * <li>Due to the lack of the callbacks mentioned previously, the
 * counter that tracks loading gets never reset. This is the
 * reason for logging only the difference of the current and last logged
 * value of the counter. Otherwise consecutive loads (such as multiple
 * {@link IMap#loadAll(boolean)} invocations) would report incorrect
 * values.
 * </ul>
 *
 * @see MapContainer#MapContainer
 */
public class MapLoadProgressTracker {
    private static final long TEN_SECS = SECONDS.toMillis(10);
    private static final int INITIAL_REPORT_TIMESTAMP = -1;

    private final String mapName;
    private final ILogger logger;

    private final AtomicLong recordsLoaded = new AtomicLong(0);
    private final AtomicReference<LoadSnapshot> lastReportedSnapshotRef;

    MapLoadProgressTracker(String mapName) {
        this(mapName, Logger.getLogger(MapLoadProgressTracker.class));
    }

    MapLoadProgressTracker(String mapName, LoggingService loggingService) {
        this(mapName, loggingService.getLogger(MapLoadProgressTracker.class));
    }

    private MapLoadProgressTracker(String mapName, ILogger logger) {
        this.mapName = mapName;
        this.lastReportedSnapshotRef = new AtomicReference<LoadSnapshot>(new LoadSnapshot(0, INITIAL_REPORT_TIMESTAMP));
        this.logger = logger;
    }

    public void onBatchLoaded(int records) {
        recordsLoaded.addAndGet(records);

        LoadSnapshot lastReportedSnapshot = lastReportedSnapshotRef.get();
        long now = System.currentTimeMillis();
        long lastReportTimestamp = lastReportedSnapshot.timestamp;
        long lastReportRecordsLoaded = lastReportedSnapshot.recordsLoaded;

        long recordsLoadedSinceLastReport = recordsLoaded.get() - lastReportRecordsLoaded;
        long elapsedMillisSinceLastReport = now - lastReportTimestamp;

        if (lastReportTimestamp == INITIAL_REPORT_TIMESTAMP) {
            LoadSnapshot newSnapshot = new LoadSnapshot(0, now);
            lastReportedSnapshotRef.compareAndSet(lastReportedSnapshot, newSnapshot);
        } else if (elapsedMillisSinceLastReport >= TEN_SECS && recordsLoadedSinceLastReport > 0) {
            LoadSnapshot newSnapshot = new LoadSnapshot(recordsLoaded.get(), now);
            if (lastReportedSnapshotRef.compareAndSet(lastReportedSnapshot, newSnapshot)) {
                logger.info("Loaded " + recordsLoadedSinceLastReport + " records for the map " + mapName + " in the last "
                        + MILLISECONDS.toSeconds(elapsedMillisSinceLastReport) + " seconds");
            }
        }
    }

    private static final class LoadSnapshot {
        private final long recordsLoaded;
        private final long timestamp;

        private LoadSnapshot(long recordsLoaded, long timestamp) {
            this.recordsLoaded = recordsLoaded;
            this.timestamp = timestamp;
        }
    }
}
