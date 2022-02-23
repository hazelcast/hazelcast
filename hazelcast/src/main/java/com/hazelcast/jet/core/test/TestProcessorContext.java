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
import com.hazelcast.core.ManagedContext;
import com.hazelcast.jet.config.JobConfig;
import com.hazelcast.jet.config.ProcessingGuarantee;
import com.hazelcast.jet.core.Processor;
import com.hazelcast.logging.ILogger;

import javax.annotation.Nonnull;
import java.util.Map;

/**
 * {@link Processor.Context} implementation suitable to be used in tests.
 *
 * @since Jet 3.0
 */
public class TestProcessorContext extends TestProcessorSupplierContext implements Processor.Context {

    private int localProcessorIndex;
    private int globalProcessorIndex;

    /**
     * Constructor with default values.
     */
    public TestProcessorContext() {
        localProcessorIndex = 0;
        globalProcessorIndex = 0;
    }

    @Override
    public int localProcessorIndex() {
        assert localProcessorIndex >= 0 && localProcessorIndex < localParallelism()
                : "localProcessorIndex should be in range 0.." + (localParallelism() - 1);
        return localProcessorIndex;
    }

    @Override
    public int globalProcessorIndex() {
        assert globalProcessorIndex >= 0 && globalProcessorIndex < totalParallelism()
                : "globalProcessorIndex should be in range 0.." + (totalParallelism() - 1);
        return globalProcessorIndex;
    }

    /**
     * Set the local processor index
     */
    public TestProcessorContext setLocalProcessorIndex(int localProcessorIndex) {
        this.localProcessorIndex = localProcessorIndex;
        return this;
    }

    /**
     * Set the global processor index
     */
    public TestProcessorContext setGlobalProcessorIndex(int globalProcessorIndex) {
        this.globalProcessorIndex = globalProcessorIndex;
        return this;
    }

    @Nonnull @Override
    public TestProcessorContext setLogger(@Nonnull ILogger logger) {
        return (TestProcessorContext) super.setLogger(logger);
    }

    @Nonnull @Override
    public TestProcessorContext setHazelcastInstance(@Nonnull HazelcastInstance instance) {
        return (TestProcessorContext) super.setHazelcastInstance(instance);
    }

    @Nonnull @Override
    public TestProcessorContext setJobConfig(@Nonnull JobConfig jobConfig) {
        return (TestProcessorContext) super.setJobConfig(jobConfig);
    }

    @Nonnull @Override
    public TestProcessorContext setTotalParallelism(int totalParallelism) {
        return (TestProcessorContext) super.setTotalParallelism(totalParallelism);
    }

    @Nonnull @Override
    public TestProcessorContext setLocalParallelism(int localParallelism) {
        return (TestProcessorContext) super.setLocalParallelism(localParallelism);
    }

    @Nonnull @Override
    public TestProcessorContext setVertexName(@Nonnull String vertexName) {
        return (TestProcessorContext) super.setVertexName(vertexName);
    }

    @Nonnull @Override
    public TestProcessorContext setProcessingGuarantee(@Nonnull ProcessingGuarantee processingGuarantee) {
        return (TestProcessorContext) super.setProcessingGuarantee(processingGuarantee);
    }

    @Nonnull @Override
    public TestProcessorContext setIsLightJob(boolean isLightJob) {
        return (TestProcessorContext) super.setIsLightJob(isLightJob);
    }

    @Nonnull @Override
    public TestProcessorContext setPartitionAssignment(Map<Address, int[]> partitionAssignment) {
        return (TestProcessorContext) super.setPartitionAssignment(partitionAssignment);
    }

    @Nonnull @Override
    public TestProcessorContext setManagedContext(@Nonnull ManagedContext managedContext) {
        return (TestProcessorContext) super.setManagedContext(managedContext);
    }

    @Override
    protected String loggerName() {
        return vertexName() + "#" + globalProcessorIndex;
    }
}
