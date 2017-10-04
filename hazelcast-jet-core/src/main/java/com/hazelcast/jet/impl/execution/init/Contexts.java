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

package com.hazelcast.jet.impl.execution.init;

import com.hazelcast.jet.JetInstance;
import com.hazelcast.jet.core.Processor;
import com.hazelcast.jet.core.ProcessorMetaSupplier.Context;
import com.hazelcast.jet.core.ProcessorSupplier;
import com.hazelcast.logging.ILogger;
import com.hazelcast.spi.serialization.SerializationService;

import javax.annotation.Nonnull;
import java.util.concurrent.CompletableFuture;

public final class Contexts {

    private Contexts() {
    }

    public static class ProcCtx implements Processor.Context {

        private final JetInstance instance;
        private final ILogger logger;
        private final String vertexName;
        private final int index;
        private final SerializationService serService;
        private CompletableFuture<Void> jobFuture;
        private final boolean snapshottingEnabled;

        public ProcCtx(JetInstance instance, SerializationService serService,
                       ILogger logger, String vertexName, int index, boolean snapshottingEnabled) {
            this.instance = instance;
            this.serService = serService;
            this.logger = logger;
            this.vertexName = vertexName;
            this.index = index;
            this.snapshottingEnabled = snapshottingEnabled;
        }

        @Nonnull
        @Override
        public JetInstance jetInstance() {
            return instance;
        }

        @Nonnull
        @Override
        public ILogger logger() {
            return logger;
        }

        @Override
        public int globalProcessorIndex() {
            return index;
        }

        @Nonnull
        @Override
        public String vertexName() {
            return vertexName;
        }

        /**
         * Note that method is marked sa {@link Nonnull}, however, it's only
         * non-null after {@link #initJobFuture(CompletableFuture)} is called,
         * which means it's <i>practically non-null</i>.
         */
        @Nonnull
        @Override
        public CompletableFuture<Void> jobFuture() {
            return jobFuture;
        }

        @Override
        public boolean snapshottingEnabled() {
            return snapshottingEnabled;
        }

        public void initJobFuture(CompletableFuture<Void> jobFuture) {
            assert this.jobFuture == null : "jobFuture already initialized";
            this.jobFuture = jobFuture;
        }

        public SerializationService getSerializationService() {
            return serService;
        }
    }

    static class ProcSupplierCtx implements ProcessorSupplier.Context {
        private final JetInstance instance;
        private final int perNodeParallelism;
        private final boolean snapshottingEnabled;
        private final ILogger logger;

        ProcSupplierCtx(JetInstance instance, ILogger logger, int perNodeParallelism, boolean snapshottingEnabled) {
            this.instance = instance;
            this.perNodeParallelism = perNodeParallelism;
            this.snapshottingEnabled = snapshottingEnabled;
            this.logger = logger;
        }

        @Nonnull
        @Override
        public JetInstance jetInstance() {
            return instance;
        }

        @Override
        public int localParallelism() {
            return perNodeParallelism;
        }

        @Override
        public boolean snapshottingEnabled() {
            return snapshottingEnabled;
        }

        @Nonnull @Override
        public ILogger logger() {
            return logger;
        }
    }

    static class MetaSupplierCtx implements Context {
        private final JetInstance jetInstance;
        private final ILogger logger;
        private final int totalParallelism;
        private final int localParallelism;
        private final boolean snapshottingEnabled;

        MetaSupplierCtx(JetInstance jetInstance, ILogger logger, int totalParallelism, int localParallelism, boolean
                snapshottingEnabled) {
            this.jetInstance = jetInstance;
            this.logger = logger;
            this.totalParallelism = totalParallelism;
            this.localParallelism = localParallelism;
            this.snapshottingEnabled = snapshottingEnabled;
        }

        @Nonnull
        @Override
        public JetInstance jetInstance() {
            return jetInstance;
        }

        @Override
        public int totalParallelism() {
            return totalParallelism;
        }

        @Override
        public int localParallelism() {
            return localParallelism;
        }

        @Override
        public boolean snapshottingEnabled() {
            return snapshottingEnabled;
        }

        @Nonnull @Override
        public ILogger logger() {
            return logger;
        }

    }
}

