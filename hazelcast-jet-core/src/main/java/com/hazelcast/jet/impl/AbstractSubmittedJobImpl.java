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

import com.hazelcast.jet.DAG;
import com.hazelcast.jet.JetInstance;
import com.hazelcast.jet.config.JobConfig;
import com.hazelcast.logging.ILogger;
import com.hazelcast.nio.Address;

import javax.annotation.Nonnull;
import java.util.concurrent.Future;

public abstract class AbstractSubmittedJobImpl extends AbstractJobImpl {

    protected final DAG dag;
    protected final JobConfig config;
    private final JobRepository jobRepository;
    private Long jobId;

    AbstractSubmittedJobImpl(JetInstance jetInstance, ILogger logger, DAG dag, JobConfig config) {
        super(logger);
        this.jobRepository = new JobRepository(jetInstance, null);
        this.dag = dag;
        this.config = config;
    }

    @Nonnull
    @Override
    public final Future<Void> getFuture() {
        if (jobId == null) {
            throw new IllegalStateException("Job is not initialized yet!");
        }
        return super.getFuture();
    }

    @Override
    public final long getJobId() {
        if (jobId == null) {
            throw new IllegalStateException("ID not yet assigned");
        }

        return jobId;
    }

    /**
     * Create the job record and upload all the resources
     *
     * Also sends a JoinOp to ensure that the job is started as soon as possible
     */
    final void init() {
        if (jobId != null) {
            throw new IllegalStateException("Job already started");
        }

        Address masterAddress = getMasterAddress();
        if (masterAddress == null) {
            throw new IllegalStateException("Master address is null");
        }

        jobId = jobRepository.uploadJobResources(config);

        super.init();
    }

}
