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

package com.hazelcast.jet.core.test;

import com.hazelcast.cluster.Address;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.instance.impl.HazelcastInstanceImpl;
import com.hazelcast.instance.impl.HazelcastInstanceProxy;
import com.hazelcast.jet.JetInstance;
import com.hazelcast.jet.config.JobConfig;
import com.hazelcast.jet.config.ProcessingGuarantee;
import com.hazelcast.jet.core.ProcessorMetaSupplier;
import com.hazelcast.jet.impl.execution.init.ExecutionPlanBuilder;
import com.hazelcast.jet.impl.util.Util;
import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;
import com.hazelcast.spi.impl.NodeEngineImpl;

import javax.annotation.Nonnull;
import java.net.UnknownHostException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

import static com.hazelcast.jet.config.ProcessingGuarantee.NONE;
import static java.util.stream.Collectors.toMap;

/**
 * {@link ProcessorMetaSupplier.Context} implementation suitable to be used
 * in tests.
 *
 * @since Jet 3.0
 */
public class TestProcessorMetaSupplierContext implements ProcessorMetaSupplier.Context {

    protected ILogger logger;
    private HazelcastInstance instance;
    private long jobId = 1;
    private long executionId = 1;
    private JobConfig jobConfig = new JobConfig();
    private int totalParallelism = 1;
    private int localParallelism = 1;
    private String vertexName = "testVertex";
    private ProcessingGuarantee processingGuarantee = NONE;
    private long maxProcessorAccumulatedRecords = Long.MAX_VALUE;
    private boolean isLightJob;
    private Map<Address, int[]> partitionAssignment = Collections.unmodifiableMap(new HashMap<Address, int[]>() {{
        try {
            put(new Address("1.2.3.4", 1), new int[]{0});
        } catch (UnknownHostException e) {
            throw new RuntimeException(e);
        }
    }});
    private ClassLoader classLoader;

    @Nonnull @Override
    public HazelcastInstance hazelcastInstance() {
        return instance;
    }

    @Nonnull @Override
    @Deprecated
    public JetInstance jetInstance() {
        return (JetInstance) instance.getJet();
    }

    /**
     * Sets the Hazelcast instance.
     */
    @Nonnull
    public TestProcessorMetaSupplierContext setHazelcastInstance(@Nonnull HazelcastInstance instance) {
        this.instance = instance;
        if (this.instance instanceof HazelcastInstanceProxy || this.instance instanceof HazelcastInstanceImpl) {
            NodeEngineImpl nodeEngine = Util.getNodeEngine(this.instance);
            this.partitionAssignment = ExecutionPlanBuilder.getPartitionAssignment(nodeEngine,
                    Util.getMembersView(nodeEngine).getMembers())
                    .entrySet().stream().collect(toMap(en -> en.getKey().getAddress(), Entry::getValue));
        }
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
     * Sets the config for the job.
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
    public TestProcessorMetaSupplierContext setProcessingGuarantee(@Nonnull ProcessingGuarantee processingGuarantee) {
        this.processingGuarantee = processingGuarantee;
        return this;
    }

    @Override
    public long maxProcessorAccumulatedRecords() {
        return maxProcessorAccumulatedRecords;
    }

    public void setMaxProcessorAccumulatedRecords(long maxProcessorAccumulatedRecords) {
        this.maxProcessorAccumulatedRecords = maxProcessorAccumulatedRecords;
    }

    @Override
    public boolean isLightJob() {
        return isLightJob;
    }

    /**
     * Sets the isLightJob flag.
     */
    @Nonnull
    public TestProcessorMetaSupplierContext setIsLightJob(boolean isLightJob) {
        this.isLightJob = isLightJob;
        return this;
    }

    @Override
    public Map<Address, int[]> partitionAssignment() {
        return partitionAssignment;
    }

    /**
     * Sets the partition assignment.
     */
    @Nonnull
    public TestProcessorMetaSupplierContext setPartitionAssignment(Map<Address, int[]> partitionAssignment) {
        this.partitionAssignment = partitionAssignment;
        return this;
    }

    @Override
    public ClassLoader classLoader() {
        return classLoader;
    }

    @Nonnull
    public TestProcessorMetaSupplierContext setClassLoader(ClassLoader classLoader) {
        this.classLoader = classLoader;
        return this;
    }
}
