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

package com.hazelcast.mapreduce.impl.task;

import com.hazelcast.mapreduce.JobPartitionState;
import com.hazelcast.mapreduce.JobProcessInformation;
import com.hazelcast.nio.Address;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

import java.util.Arrays;
import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;
import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;

import static com.hazelcast.mapreduce.JobPartitionState.State.CANCELLED;
import static com.hazelcast.util.Preconditions.isNotNull;

/**
 * This class controls all partition states and is capable of atomically updating those states. It also
 * collects information about the processed records.
 */
public class JobProcessInformationImpl
        implements JobProcessInformation {

    private static final AtomicReferenceFieldUpdater<JobProcessInformationImpl, JobPartitionState[]>
            PARTITION_STATE = AtomicReferenceFieldUpdater.newUpdater(JobProcessInformationImpl.class,
            JobPartitionState[].class, "partitionStates");
    private static final AtomicIntegerFieldUpdater<JobProcessInformationImpl> PROCESSED_RECORDS =
            AtomicIntegerFieldUpdater.newUpdater(JobProcessInformationImpl.class, "processedRecords");

    private final JobSupervisor supervisor;

    // This field is only accessed through the updater
    private volatile int processedRecords;

    private volatile JobPartitionState[] partitionStates;

    public JobProcessInformationImpl(int partitionCount, JobSupervisor supervisor) {
        this.supervisor = supervisor;
        this.partitionStates = new JobPartitionState[partitionCount];
    }

    @Override
    @SuppressFBWarnings(value = "EI_EXPOSE_REP", justification = "Exposed array is used a lot on internals"
            + " and is explicitly exposed for speed / object creation reasons."
            + " It is never exposed to the end user (either through serialization cycle"
            + " or by hiding in through a wrapper class")
    public JobPartitionState[] getPartitionStates() {
        return partitionStates;
    }

    @Override
    public int getProcessedRecords() {
        return processedRecords;
    }

    public void addProcessedRecords(int records) {
        PROCESSED_RECORDS.addAndGet(this, records);
    }

    public void cancelPartitionState() {
        JobPartitionState[] oldPartitionStates = this.partitionStates;
        JobPartitionState[] newPartitionStates = new JobPartitionState[oldPartitionStates.length];
        for (int i = 0; i < newPartitionStates.length; i++) {
            Address owner = oldPartitionStates[i] != null ? oldPartitionStates[i].getOwner() : null;
            newPartitionStates[i] = new JobPartitionStateImpl(owner, CANCELLED);
        }

        this.partitionStates = newPartitionStates;
    }

    public boolean updatePartitionState(int partitionId, JobPartitionState oldPartitionState,
                                        JobPartitionState newPartitionState) {

        while (true) {
            JobPartitionState[] oldPartitionStates = getPartitionStates();
            if (oldPartitionStates[partitionId] != oldPartitionState) {
                return false;
            }

            JobPartitionState[] newPartitionStates = Arrays.copyOf(oldPartitionStates, oldPartitionStates.length);
            newPartitionStates[partitionId] = newPartitionState;
            if (updatePartitionState(oldPartitionStates, newPartitionStates)) {
                return true;
            }
        }
    }

    public boolean updatePartitionState(JobPartitionState[] oldPartitionStates, JobPartitionState[] newPartitionStates) {
        isNotNull(newPartitionStates, "newPartitionStates");
        if (oldPartitionStates.length != newPartitionStates.length) {
            throw new IllegalArgumentException("partitionStates need to have same length");
        }
        if (PARTITION_STATE.compareAndSet(this, oldPartitionStates, newPartitionStates)) {
            supervisor.checkFullyProcessed(this);
            return true;
        }
        return false;
    }

    @Override
    public String toString() {
        return "JobProcessInformationImpl{" + "processedRecords=" + processedRecords + ", partitionStates=" + Arrays
                .toString(partitionStates) + '}';
    }

}
