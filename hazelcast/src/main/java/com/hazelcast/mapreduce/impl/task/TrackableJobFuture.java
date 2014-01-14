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
import com.hazelcast.mapreduce.JobProcessInformation;
import com.hazelcast.mapreduce.JobTracker;
import com.hazelcast.mapreduce.TrackableJob;
import com.hazelcast.mapreduce.impl.MapReduceService;
import com.hazelcast.spi.NodeEngine;
import com.hazelcast.spi.impl.AbstractCompletableFuture;
import com.hazelcast.spi.impl.NodeEngineImpl;
import com.hazelcast.util.Clock;
import com.hazelcast.util.ValidationUtil;

import java.util.Map;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

public class TrackableJobFuture<V>
        extends AbstractCompletableFuture<V>
        implements TrackableJob<V> {

    private final String name;
    private final String jobId;
    private final JobTracker jobTracker;
    private final Collator collator;
    private final MapReduceService mapReduceService;

    private volatile boolean cancelled;

    public TrackableJobFuture(String name, String jobId, JobTracker jobTracker,
                              NodeEngine nodeEngine, Collator collator) {
        super(nodeEngine, nodeEngine.getLogger(TrackableJobFuture.class));
        this.name = name;
        this.jobId = jobId;
        this.jobTracker = jobTracker;
        this.collator = collator;
        this.mapReduceService = ((NodeEngineImpl) nodeEngine).getService(MapReduceService.SERVICE_NAME);
    }

    @Override
    public void setResult(Object result) {
        // If collator is available we need to execute it now
        if (collator != null) {
            result = collator.collate(((Map) result).entrySet());
        }
        super.setResult(result);
    }

    @Override
    public boolean cancel(boolean mayInterruptIfRunning) {
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
    public V get(long timeout, TimeUnit unit) throws InterruptedException, ExecutionException, TimeoutException {
        ValidationUtil.isNotNull(unit, "unit");
        long deadline = timeout == 0L ? -1 : Clock.currentTimeMillis() + unit.toMillis(timeout);
        for (; ; ) {
            try {
                Thread.sleep(100);
            } catch (Exception e) {
                if (e instanceof InterruptedException) {
                    throw (InterruptedException) e;
                }
            }

            if (isDone()) {
                break;
            }

            long delta = deadline - Clock.currentTimeMillis();
            if (delta <= 0L) {
                throw new TimeoutException("timeout reached");
            }
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
        return supervisor.getJobProcessInformation();
    }

}
