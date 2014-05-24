/*
 * Copyright (c) 2008-2013, Hazelcast, Inc. All Rights Reserved.
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
import com.hazelcast.util.ValidationUtil;
import edu.umd.cs.findbugs.annotations.SuppressWarnings;

import java.util.Arrays;
import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;
import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;

import static com.hazelcast.mapreduce.JobPartitionState.State.CANCELLED;

/**
 * This class controls all partition states and is capable of atomically updating those states. It also
 * collects information about the processed records.
 */
public class JobProcessInformationImpl
        implements JobProcessInformation {

    //CHECKSTYLE:OFF
    private static final AtomicReferenceFieldUpdater<JobProcessInformationImpl, JobPartitionState[]> PARTITION_STATE_UPDATER = //
            AtomicReferenceFieldUpdater.newUpdater(JobProcessInformationImpl.class, JobPartitionState[].class, "partitionStates");
    private static final AtomicIntegerFieldUpdater<JobProcessInformationImpl> PROCESSED_RECORDS_UPDATER = AtomicIntegerFieldUpdater
            .newUpdater(JobProcessInformationImpl.class, "processedRecords");
    //CHECKSTYLE:ON

    private final JobSupervisor supervisor;

    private volatile int processedRecords = 0;
    private volatile JobPartitionState[] partitionStates;

    public JobProcessInformationImpl(int partitionCount, JobSupervisor supervisor) {
        this.supervisor = supervisor;
        this.partitionStates = new JobPartitionState[partitionCount];
    }

    @Override
    // Expose warning suppressed, this exposed array is used a lot on internals
    // and is explicitly exposed for speed / object creation reasons.
    // It is never exposed to the end user (either through serialization cycle
    // or by hiding in through a wrapper class
    @SuppressWarnings("EI_EXPOSE_REP")
    public JobPartitionState[] getPartitionStates() {
        return partitionStates;
    }

    @Override
    public int getProcessedRecords() {
        return processedRecords;
    }

    public void addProcessedRecords(int records) {
        PROCESSED_RECORDS_UPDATER.addAndGet(this, records);
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

    public void resetPartitionState() {
        JobPartitionState[] oldPartitionStates = this.partitionStates;
        JobPartitionState[] newPartitionStates = new JobPartitionState[oldPartitionStates.length];
        for (int i = 0; i < newPartitionStates.length; i++) {
            newPartitionStates[i] = null;
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
        ValidationUtil.isNotNull(newPartitionStates, "newPartitionStates");
        if (oldPartitionStates.length != newPartitionStates.length) {
            throw new IllegalArgumentException("partitionStates need to have same length");
        }
        if (PARTITION_STATE_UPDATER.compareAndSet(this, oldPartitionStates, newPartitionStates)) {
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
