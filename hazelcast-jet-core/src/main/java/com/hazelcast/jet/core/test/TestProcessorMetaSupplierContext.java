/*
 * Copyright (c) 2008-2019, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.jet.core.test;

import com.hazelcast.jet.JetInstance;
import com.hazelcast.jet.config.JobConfig;
import com.hazelcast.jet.config.ProcessingGuarantee;
import com.hazelcast.jet.core.ProcessorMetaSupplier;
import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;

import javax.annotation.Nonnull;

/**
 * {@link ProcessorMetaSupplier.Context} implementation suitable to be used
 * in tests.
 *
 * @since 3.0
 */
public class TestProcessorMetaSupplierContext implements ProcessorMetaSupplier.Context {

    protected ILogger logger;

    private JetInstance jetInstance;
    private long jobId = 1;
    private long executionId = 1;
    private JobConfig jobConfig = new JobConfig();
    private int totalParallelism = 1;
    private int localParallelism = 1;
    private String vertexName = "testVertex";
    private ProcessingGuarantee processingGuarantee;

    @Nonnull @Override
    public JetInstance jetInstance() {
        return jetInstance;
    }

    /**
     * Sets the jet instance.
     */
    @Nonnull
    public TestProcessorMetaSupplierContext setJetInstance(@Nonnull JetInstance jetInstance) {
        this.jetInstance = jetInstance;
        return this;
    }

    @Override
    public long jobId() {
        return jobId;
    }

    /**
     * Sets the job ID.
     */
    public TestProcessorMetaSupplierContext setJobId(long jobId) {
        this.jobId = jobId;
        return this;
    }

    @Override
    public long executionId() {
        return executionId;
    }

    /**
     * Sets the execution ID.
     */
    public TestProcessorMetaSupplierContext setExecutionId(long executionId) {
        this.executionId = executionId;
        return this;
    }

    @Nonnull @Override
    public JobConfig jobConfig() {
        return jobConfig;
    }

    /**
     * Sets the job name.
     */
    public TestProcessorMetaSupplierContext setJobConfig(@Nonnull JobConfig jobConfig) {
        this.jobConfig = jobConfig;
        return this;
    }

    @Override
    public int totalParallelism() {
        return totalParallelism;
    }

    /**
     * Sets the total parallelism.
     */
    @Nonnull
    public TestProcessorMetaSupplierContext setTotalParallelism(int totalParallelism) {
        this.totalParallelism = totalParallelism;
        return this;
    }

    @Override
    public int localParallelism() {
        assert totalParallelism % localParallelism == 0 :
                "totalParallelism=" + totalParallelism + " not divisible with localParallelism=" + localParallelism;
        return localParallelism;
    }

    /**
     * Sets local parallelism.
     */
    @Nonnull
    public TestProcessorMetaSupplierContext setLocalParallelism(int localParallelism) {
        this.localParallelism = localParallelism;
        return this;
    }

    @Nonnull @Override
    public ILogger logger() {
        if (logger == null) {
            logger = Logger.getLogger(loggerName());
        }
        return logger;
    }

    /**
     * Sets the logger.
     */
    public TestProcessorMetaSupplierContext setLogger(@Nonnull ILogger logger) {
        this.logger = logger;
        return this;
    }

    @Override
    public int memberCount() {
        return totalParallelism() / localParallelism();
    }

    @Nonnull @Override
    public String vertexName() {
        return vertexName;
    }

    /**
     * Sets the vertex name.
     */
    @Nonnull
    public TestProcessorMetaSupplierContext setVertexName(@Nonnull String vertexName) {
        this.vertexName = vertexName;
        return this;
    }

    protected String loggerName() {
        return vertexName() + "#PMS";
    }

    @Override
    public ProcessingGuarantee processingGuarantee() {
        return processingGuarantee;
    }

    /**
     * Sets the processing guarantee.
     */
    @Nonnull
    public TestProcessorMetaSupplierContext setProcessingGuarantee(ProcessingGuarantee processingGuarantee) {
        this.processingGuarantee = processingGuarantee;
        return this;
    }
}
