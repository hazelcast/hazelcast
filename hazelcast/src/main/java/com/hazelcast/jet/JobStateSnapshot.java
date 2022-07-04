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

package com.hazelcast.jet;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.jet.impl.JobRepository;
import com.hazelcast.jet.impl.SnapshotValidationRecord;
import com.hazelcast.spi.annotation.PrivateApi;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import static com.hazelcast.jet.impl.JobRepository.exportedSnapshotMapName;

/**
 * A handle to an exported state snapshot created using {@link
 * Job#exportSnapshot(String)}.
 *
 * @since Jet 3.0
 */
public final class JobStateSnapshot {

    private final HazelcastInstance instance;
    private final String name;
    private final SnapshotValidationRecord snapshotValidationRecord;

    /**
     * This constructor is a private API, use {@link
     * JetService#getJobStateSnapshot(String)} instead.
     */
    @PrivateApi
    public JobStateSnapshot(@Nonnull HazelcastInstance instance, @Nonnull String name, @Nonnull SnapshotValidationRecord record) {
        this.instance = instance;
        this.name = name;
        this.snapshotValidationRecord = record;
    }

    /**
     * Returns the snapshot name. This is the name that was given to {@link
     * Job#exportSnapshot(String)}.
     */
    @Nonnull
    public String name() {
        return name;
    }

    /**
     * Returns the time the snapshot was created.
     */
    public long creationTime() {
        return snapshotValidationRecord.creationTime();
    }

    /**
     * Returns the job ID of the job the snapshot was originally exported from.
     */
    public long jobId() {
        return snapshotValidationRecord.jobId();
    }

    /**
     * Returns the job name of the job the snapshot was originally exported
     * from.
     */
    @Nullable
    public String jobName() {
        return snapshotValidationRecord.jobName();
    }

    /**
     * Returns the size in bytes of the payload data of the state snapshot.
     * Doesn't include storage overhead and especially doesn't account for
     * backup copies.
     */
    public long payloadSize() {
        return snapshotValidationRecord.numBytes();
    }

    /**
     * Returns the JSON representation of the DAG of the job this snapshot was
     * created from.
     */
    @Nonnull
    public String dagJsonString() {
        return snapshotValidationRecord.dagJsonString();
    }

    /**
     * Destroy the underlying distributed object.
     */
    public void destroy() {
        instance.getMap(exportedSnapshotMapName(name)).destroy();
        instance.getMap(JobRepository.EXPORTED_SNAPSHOTS_DETAIL_CACHE).delete(name);
    }
}
