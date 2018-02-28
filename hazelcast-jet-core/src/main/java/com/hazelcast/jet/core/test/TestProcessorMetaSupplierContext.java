/*
 * Copyright (c) 2008-2018, Hazelcast, Inc. All Rights Reserved.
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
import com.hazelcast.jet.core.ProcessorMetaSupplier;
import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;

import javax.annotation.Nonnull;

/**
 * {@link ProcessorMetaSupplier.Context} implementation suitable to be used
 * in tests.
 */
public class TestProcessorMetaSupplierContext implements ProcessorMetaSupplier.Context {

    protected ILogger logger;

    private JetInstance jetInstance;
    private int totalParallelism = 1;
    private int localParallelism = 1;
    private String vertexName = "testVertex";

    @Nonnull @Override
    public JetInstance jetInstance() {
        return jetInstance;
    }

    /**
     * Set the jet instance.
     */
    @Nonnull
    public TestProcessorMetaSupplierContext setJetInstance(@Nonnull JetInstance jetInstance) {
        this.jetInstance = jetInstance;
        return this;
    }

    @Override
    public int totalParallelism() {
        return totalParallelism;
    }

    /**
     * Set total parallelism.
     */
    @Nonnull
    public TestProcessorMetaSupplierContext setTotalParallelism(int totalParallelism) {
        this.totalParallelism = totalParallelism;
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
     * Set the logger.
     */
    public TestProcessorMetaSupplierContext setLogger(@Nonnull ILogger logger) {
        this.logger = logger;
        return this;
    }

    @Override
    public int localParallelism() {
        return localParallelism;
    }

    /**
     * Set local parallelism.
     */
    @Nonnull
    public TestProcessorMetaSupplierContext setLocalParallelism(int localParallelism) {
        this.localParallelism = localParallelism;
        return this;
    }

    @Nonnull @Override
    public String vertexName() {
        return vertexName;
    }

    /**
     * Set the vertex name.
     */
    @Nonnull
    public TestProcessorMetaSupplierContext setVertexName(@Nonnull String vertexName) {
        this.vertexName = vertexName;
        return this;
    }

    protected String loggerName() {
        return vertexName() + "#PMS";
    }
}
