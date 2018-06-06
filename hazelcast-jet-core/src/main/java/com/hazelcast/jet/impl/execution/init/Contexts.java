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

package com.hazelcast.jet.impl.execution.init;

import com.hazelcast.jet.JetInstance;
import com.hazelcast.jet.config.ProcessingGuarantee;
import com.hazelcast.jet.core.Processor;
import com.hazelcast.jet.core.ProcessorMetaSupplier;
import com.hazelcast.jet.core.ProcessorSupplier;
import com.hazelcast.logging.ILogger;
import com.hazelcast.spi.serialization.SerializationService;

import javax.annotation.Nonnull;

public final class Contexts {

    private Contexts() {
    }

    static class MetaSupplierCtx implements ProcessorMetaSupplier.Context {
        private final JetInstance jetInstance;
        private final ILogger logger;
        private final String vertexName;
        private final int localParallelism;
        private final int totalParallelism;
        private final int memberCount;

        MetaSupplierCtx(
                JetInstance jetInstance, ILogger logger, String vertexName, int localParallelism, int totalParallelism,
                int memberCount) {
            this.jetInstance = jetInstance;
            this.logger = logger;
            this.vertexName = vertexName;
            this.totalParallelism = totalParallelism;
            this.localParallelism = localParallelism;
            this.memberCount = memberCount;
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
        public int memberCount() {
            return memberCount;
        }

        @Nonnull @Override
        public String vertexName() {
            return vertexName;
        }

        @Nonnull @Override
        public ILogger logger() {
            return logger;
        }
    }

    static class ProcSupplierCtx extends MetaSupplierCtx implements ProcessorSupplier.Context {

        private final int memberIndex;

        ProcSupplierCtx(
                JetInstance jetInstance, ILogger logger, String vertexName, int localParallelism, int totalParallelism,
                int memberIndex, int memberCount) {
            super(jetInstance, logger, vertexName, localParallelism, totalParallelism, memberCount);
            this.memberIndex = memberIndex;
        }

        @Override
        public int memberIndex() {
            return memberIndex;
        }
    }

    public static class ProcCtx extends ProcSupplierCtx implements Processor.Context {

        private final int localProcessorIndex;
        private final int globalProcessorIndex;
        private final SerializationService serService;
        private final ProcessingGuarantee processingGuarantee;

        public ProcCtx(JetInstance instance, SerializationService serService, ILogger logger, String vertexName,
                       int localProcessorIndex, int globalProcessorIndex, ProcessingGuarantee processingGuarantee,
                       int localParallelism, int memberIndex, int memberCount) {
            super(instance, logger, vertexName, localParallelism, memberCount * localParallelism,
                    memberIndex, memberCount);
            this.serService = serService;
            this.localProcessorIndex = localProcessorIndex;
            this.globalProcessorIndex = globalProcessorIndex;
            this.processingGuarantee = processingGuarantee;
        }

        @Override
        public int localProcessorIndex() {
            return localProcessorIndex;
        }

        @Override
        public int globalProcessorIndex() {
            return globalProcessorIndex;
        }

        @Override
        public ProcessingGuarantee processingGuarantee() {
            return processingGuarantee;
        }

        public SerializationService getSerializationService() {
            return serService;
        }
    }
}
