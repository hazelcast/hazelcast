/*
 * Copyright (c) 2008-2016, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.jet.cascading.planner;

import cascading.flow.planner.BaseFlowStep;
import cascading.flow.planner.FlowStepJob;
import cascading.management.state.ClientState;
import cascading.stats.FlowNodeStats;
import cascading.stats.FlowStepStats;
import com.hazelcast.jet.DAG;
import com.hazelcast.jet.JetEngine;
import com.hazelcast.jet.Job;
import com.hazelcast.jet.cascading.JetFlowProcess;
import com.hazelcast.jet.config.JobConfig;
import com.hazelcast.jet.impl.statemachine.job.JobState;
import com.hazelcast.jet.impl.util.JetUtil;
import com.hazelcast.logging.ILogger;
import com.hazelcast.util.UuidUtil;

import java.io.IOException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.locks.LockSupport;

public class JetFlowStepJob extends FlowStepJob<JobConfig> {
    public static final int POLLING_INTERVAL = 100;
    public static final int BLOCK_FOR_COMPLETED_CHILD_DETAIL_DURATION = 10000;
    public static final int STATS_STORE_INTERVAL = 10000;
    private final JetFlowProcess process;
    private final DAG dag;
    private Future jobFuture;
    private Job job;
    private final ILogger logger;
    private Throwable exception;


    public JetFlowStepJob(JetFlowProcess process, DAG dag, ClientState clientState,
                          JobConfig jobConfiguration,
                          BaseFlowStep<JobConfig> flowStep) {
        super(clientState,
                jobConfiguration,
                flowStep, POLLING_INTERVAL, STATS_STORE_INTERVAL, BLOCK_FOR_COMPLETED_CHILD_DETAIL_DURATION);
        this.process = process;
        this.dag = dag;
        logger = process.getHazelcastInstance().getLoggingService().getLogger(FlowStepJob.class);
    }

    @Override
    protected FlowStepStats createStepStats(ClientState clientState) {
        return new JetFlowStepStats(flowStep, clientState);
    }

    @Override
    protected void internalBlockOnStop() throws IOException {
        try {
            if (jobFuture != null) {
                while (!jobFuture.isDone() && (job.getJobState() != JobState.EXECUTION_IN_PROGRESS)) {
                    LockSupport.parkNanos(TimeUnit.MILLISECONDS.toNanos(POLLING_INTERVAL));
                }

                if (!jobFuture.isDone()) {
                    job.interrupt().get();
                }
            }
        } catch (Throwable e) {
            throw JetUtil.reThrow(e);
        }
    }

    @Override
    protected boolean isRemoteExecution() {
        return true;
    }

    @Override
    protected String internalJobId() {
        return job.getName();
    }

    @Override
    protected boolean internalNonBlockingIsSuccessful() throws IOException {
        try {
            jobFuture.get(0, TimeUnit.SECONDS);
            return true;
        } catch (InterruptedException e) {
            return false;
        } catch (ExecutionException e) {
            exception = e.getCause();
            return false;
        } catch (TimeoutException e) {
            return false;
        }
    }

    @Override
    protected Throwable getThrowable() {
        return exception;
    }

    @Override
    protected void internalNonBlockingStart() throws IOException {
        String name = UuidUtil.newUnsecureUuidString().replaceAll("-", "");
        job = JetEngine.getJob(process.getHazelcastInstance(), name, dag, getConfig());
        jobFuture = job.execute();
    }

    @Override
    protected void updateNodeStatus(FlowNodeStats flowNodeStats) {

    }

    @Override
    protected boolean internalNonBlockingIsComplete() throws IOException {
        return jobFuture.isDone();
    }

    @Override
    protected void dumpDebugInfo() {

    }

    @Override
    protected boolean internalIsStartedRunning() {
        return jobFuture != null;
    }

    @Override
    protected void internalCleanup() {
        if (job != null) {
            try {
                internalBlockOnStop();
            } catch (IOException ignored) {
                // ignored
            } finally {
                job.destroy();
            }
        }
    }
}
