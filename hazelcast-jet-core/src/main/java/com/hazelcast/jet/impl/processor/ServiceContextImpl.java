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

package com.hazelcast.jet.impl.processor;

import com.hazelcast.jet.JetInstance;
import com.hazelcast.jet.core.Processor;
import com.hazelcast.jet.core.ProcessorSupplier;
import com.hazelcast.jet.pipeline.ServiceFactory;
import com.hazelcast.jet.pipeline.ServiceFactory.ServiceContext;
import com.hazelcast.logging.ILogger;

import javax.annotation.Nonnull;

public class ServiceContextImpl implements ServiceContext {
    private final int memberCount;
    private final int memberIndex;
    private final int localIndex;
    private final boolean hasLocalSharing;
    private final boolean hasOrderedAsyncResponses;
    private final int maxPendingCallsPerProcessor;
    private final String vertexName;
    private final ILogger logger;
    private final JetInstance jetInstance;

    ServiceContextImpl(
            @Nonnull ServiceFactory<?> serviceFactory,
            @Nonnull ProcessorSupplier.Context context
    ) {
        this.hasLocalSharing = serviceFactory.hasLocalSharing();
        this.hasOrderedAsyncResponses = serviceFactory.hasOrderedAsyncResponses();
        this.maxPendingCallsPerProcessor = serviceFactory.maxPendingCallsPerProcessor();

        this.memberCount = context.memberCount();
        this.memberIndex = context.memberIndex();
        this.localIndex = hasLocalSharing
                ? 0
                : ((Processor.Context) context).localProcessorIndex();
        this.vertexName = context.vertexName();
        this.logger = context.logger();
        this.jetInstance = context.jetInstance();
    }

    @Override
    public int memberCount() {
        return memberCount;
    }

    @Override
    public int memberIndex() {
        return memberIndex;
    }

    @Override
    public int localIndex() {
        return localIndex;
    }

    @Override
    public boolean isSharedLocally() {
        return hasLocalSharing;
    }

    @Override
    public boolean hasOrderedAsyncResponses() {
        return hasOrderedAsyncResponses;
    }

    @Override
    public int maxPendingCallsPerProcessor() {
        return maxPendingCallsPerProcessor;
    }

    @Nonnull @Override
    public ILogger logger() {
        return logger;
    }

    @Nonnull @Override
    public JetInstance jetInstance() {
        return jetInstance;
    }

    @Nonnull @Override
    public String vertexName() {
        return vertexName;
    }
}
