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

package com.hazelcast.jet.core.test;

import com.hazelcast.jet.JetInstance;
import com.hazelcast.jet.config.ProcessingGuarantee;
import com.hazelcast.jet.core.Processor;
import com.hazelcast.logging.ILogger;

import javax.annotation.Nonnull;

import static com.hazelcast.jet.core.test.TestSupport.getLogger;

/**
 * Simple implementation of {@link Processor.Context}.
 */
public class TestProcessorContext implements Processor.Context {
    private JetInstance jetInstance;
    private ILogger logger;
    private String vertexName = "testVertex";
    private int globalProcessorIndex;
    private boolean snapshottingEnabled;
    private ProcessingGuarantee processingGuarantee = ProcessingGuarantee.EXACTLY_ONCE;

    /**
     * Constructor with default values.
     */
    public TestProcessorContext() {
        globalProcessorIndex = 0;
        logger = getLogger(vertexName + "#" + globalProcessorIndex);
    }

    @Override
    public JetInstance jetInstance() {
        return jetInstance;
    }

    /**
     * Set the jet instance.
     */
    public TestProcessorContext setJetInstance(JetInstance jetInstance) {
        this.jetInstance = jetInstance;
        return this;
    }

    @Override @Nonnull
    public ILogger logger() {
        return logger;
    }

    /**
     * Set the logger.
     */
    public TestProcessorContext setLogger(@Nonnull ILogger logger) {
        this.logger = logger;
        return this;
    }

    @Override @Nonnull
    public String vertexName() {
        return vertexName;
    }

    /**
     * Set the vertex name.
     */
    public TestProcessorContext setVertexName(@Nonnull String vertexName) {
        this.vertexName = vertexName;
        return this;
    }

    @Override
    public int globalProcessorIndex() {
        return globalProcessorIndex;
    }

    /**
     * Set the global processor index
     */
    public TestProcessorContext setGlobalProcessorIndex(int globalProcessorIndex) {
        this.globalProcessorIndex = globalProcessorIndex;
        return this;
    }

    @Override
    public boolean snapshottingEnabled() {
        return snapshottingEnabled;
    }

    /**
     * Sets if snapshotting is enabled for the job.
     */
    public TestProcessorContext setSnapshottingEnabled(boolean snapshottingEnabled) {
        this.snapshottingEnabled = snapshottingEnabled;
        return this;
    }

    @Override
    public ProcessingGuarantee processingGuarantee() {
        return processingGuarantee;
    }

    /**
     * Sets the processing guarantee.
     */
    public TestProcessorContext setProcessingGuarantee(ProcessingGuarantee processingGuarantee) {
        this.processingGuarantee = processingGuarantee;
        return this;
    }
}
