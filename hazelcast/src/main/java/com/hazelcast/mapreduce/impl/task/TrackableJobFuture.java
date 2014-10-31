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

import com.hazelcast.core.ICompletableFuture;
import com.hazelcast.mapreduce.Collator;
import com.hazelcast.mapreduce.JobCompletableFuture;
import com.hazelcast.mapreduce.JobPartitionState;
import com.hazelcast.mapreduce.JobProcessInformation;
import com.hazelcast.mapreduce.JobTracker;
import com.hazelcast.mapreduce.TrackableJob;
import com.hazelcast.mapreduce.impl.MapReduceService;
import com.hazelcast.nio.Address;
import com.hazelcast.spi.NodeEngine;
import com.hazelcast.spi.impl.AbstractCompletableFuture;
import com.hazelcast.spi.impl.NodeEngineImpl;
import com.hazelcast.util.ValidationUtil;

import java.util.Arrays;
import java.util.Map;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * This is the node based implementation of the job's reactive {@link com.hazelcast.core.ICompletableFuture}
 * and is returned to the users codebase. It hides the exposed JobPartitionState array from
 * {@link com.hazelcast.mapreduce.impl.task.JobProcessInformationImpl} by wrapping it into an adapter
 * that creates a full copy prior to returning it to the end user.
 *
 * @param <V> type of the resulting value
 */
public class TrackableJobFuture<V>
        extends AbstractCompletableFuture<V>
        implements TrackableJob<V>, JobCompletableFuture<V> {

    private final String name;
    private final String jobId;
    private final JobTracker jobTracker;
    private final Collator collator;
    private final CountDownLatch latch;
    private final MapReduceService mapReduceService;

    private volatile boolean cancelled;

    public TrackableJobFuture(String name, String jobId, JobTracker jobTracker, NodeEngine nodeEngine, Collator collator) {
        super(nodeEngine, nodeEngine.getLogger(TrackableJobFuture.class));
        this.name = name;
        this.jobId = jobId;
        this.jobTracker = jobTracker;
        this.collator = collator;
        this.latch = new CountDownLatch(1);
        this.mapReduceService = ((NodeEngineImpl) nodeEngine).getService(MapReduceService.SERVICE_NAME);
    }

    @Override
    public void setResult(Object result) {
        try {
            Object finalResult = result;
            if (finalResult instanceof Throwable && !(finalResult instanceof CancellationException)) {
                if (!(finalResult instanceof CancellationException)) {
                    finalResult = new ExecutionException((Throwable) finalResult);
                }
                super.setResult(finalResult);
                return;
            }
            // If collator is available we need to execute it now
            if (collator != null) {
                try {
                    finalResult = collator.collate(((Map) finalResult).entrySet());
                } catch (Exception e) {
                    // Possible exception while collating
                    finalResult = e;
                }
            }
            if (finalResult instanceof Throwable && !(finalResult instanceof CancellationException)) {
                finalResult = new ExecutionException((Throwable) finalResult);
            }
            super.setResult(finalResult);
        } finally {
            latch.countDown();
        }
    }

    @Override
    public boolean cancel(boolean mayInterruptIfRunning) {
        Address jobOwner = mapReduceService.getLocalAddress();
        if (!mapReduceService.registerJobSupervisorCancellation(name, jobId, jobOwner)) {
            return false;
        }
        JobSupervisor supervisor = mapReduceService.getJobSupervisor(name, jobId);
        if (supervisor == null || !supervisor.isOwnerNode()) {
            return false;
        }
        Exception exception = new CancellationException("Operation was cancelled by the user");
        cancelled = supervisor.cancelAndNotify(exception);
        return cancelled;
    }

    @Override
    public boolean isCancelled() {
        return cancelled;
    }

    @Override
    public V get(long timeout, TimeUnit unit)
            throws InterruptedException, ExecutionException, TimeoutException {
        ValidationUtil.isNotNull(unit, "unit");
        if (!latch.await(timeout, unit) || !isDone()) {
            throw new TimeoutException("timeout reached");
        }
        return getResult();
    }

    @Override
    public JobTracker getJobTracker() {
        return jobTracker;
    }

    @Override
    public String getName() {
        return name;
    }

    @Override
    public String getJobId() {
        return jobId;
    }

    @Override
    public ICompletableFuture<V> getCompletableFuture() {
        JobSupervisor supervisor = mapReduceService.getJobSupervisor(name, jobId);
        if (supervisor == null || !supervisor.isOwnerNode()) {
            return null;
        }
        return this;
    }

    @Override
    public JobProcessInformation getJobProcessInformation() {
        JobSupervisor supervisor = mapReduceService.getJobSupervisor(name, jobId);
        if (supervisor == null || !supervisor.isOwnerNode()) {
            return null;
        }
        return new JobProcessInformationAdapter(supervisor.getJobProcessInformation());
    }

    /**
     * This class is just an adapter for retrieving the JobProcess information
     * from user codebase to prevent exposing the internal array.
     */
    private static final class JobProcessInformationAdapter
            implements JobProcessInformation {
        private final JobProcessInformation processInformation;

        private JobProcessInformationAdapter(JobProcessInformation processInformation) {
            this.processInformation = processInformation;
        }

        @Override
        public JobPartitionState[] getPartitionStates() {
            JobPartitionState[] partitionStates = processInformation.getPartitionStates();
            return Arrays.copyOf(partitionStates, partitionStates.length);
        }

        @Override
        public int getProcessedRecords() {
            return processInformation.getProcessedRecords();
        }
    }

}
