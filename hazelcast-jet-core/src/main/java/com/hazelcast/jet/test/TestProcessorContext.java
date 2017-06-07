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

package com.hazelcast.jet.test;

import com.hazelcast.instance.BuildInfoProvider;
import com.hazelcast.jet.JetInstance;
import com.hazelcast.jet.Processor;
import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.LoggingServiceImpl;

import javax.annotation.Nonnull;
import java.util.concurrent.CompletableFuture;

/**
 * Simple implementation of {@link Processor.Context}.
 */
public class TestProcessorContext implements Processor.Context {
    private JetInstance jetInstance;
    private ILogger logger;
    private String vertexName = "testVertex";
    private int globalProcessorIndex;
    private CompletableFuture<Void> jobFuture = new CompletableFuture<>();

    /**
     * Constructor with default values.
     */
    public TestProcessorContext() {
        LoggingServiceImpl loggingService = new LoggingServiceImpl("test-group", null, BuildInfoProvider.getBuildInfo());
        globalProcessorIndex = 0;
        logger = loggingService.getLogger(vertexName + "#" + globalProcessorIndex);
    }

    @Override
    public JetInstance jetInstance() {
        return jetInstance;
    }

    /**
     * Set the jet instance.
     */
    public void setJetInstance(JetInstance jetInstance) {
        this.jetInstance = jetInstance;
    }

    @Override @Nonnull
    public ILogger logger() {
        return logger;
    }

    /**
     * Set the logger.
     */
    public void setLogger(@Nonnull ILogger logger) {
        this.logger = logger;
    }

    @Override @Nonnull
    public String vertexName() {
        return vertexName;
    }

    /**
     * Set the vertex name.
     */
    public void setVertexName(@Nonnull String vertexName) {
        this.vertexName = vertexName;
    }

    @Override
    public int globalProcessorIndex() {
        return globalProcessorIndex;
    }

    /**
     * Set the global processor index
     */
    public void setGlobalProcessorIndex(int globalProcessorIndex) {
        this.globalProcessorIndex = globalProcessorIndex;
    }

    @Override
    public CompletableFuture<Void> jobFuture() {
        return jobFuture;
    }

    /**
     * Set the job future.
     */
    public void setJobFuture(CompletableFuture<Void> jobFuture) {
        this.jobFuture = jobFuture;
    }
}
