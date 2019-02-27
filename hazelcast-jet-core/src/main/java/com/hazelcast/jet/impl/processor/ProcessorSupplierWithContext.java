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

import com.hazelcast.jet.core.Processor;
import com.hazelcast.jet.core.ProcessorSupplier;
import com.hazelcast.jet.function.BiFunctionEx;
import com.hazelcast.jet.pipeline.ContextFactory;

import javax.annotation.Nonnull;
import java.util.Collection;
import java.util.function.BiFunction;
import java.util.stream.Stream;

import static java.util.stream.Collectors.toList;

/**
 * Common processor supplier for transform-using-context processors
 */
public final class ProcessorSupplierWithContext<C> implements ProcessorSupplier {

    static final long serialVersionUID = 1L;

    private final ContextFactory<C> contextFactory;
    private BiFunction<ContextFactory<C>, C, Processor> createProcessorFn;
    private transient C contextObject;

    private ProcessorSupplierWithContext(
            @Nonnull ContextFactory<C> contextFactory,
            @Nonnull BiFunction<ContextFactory<C>, C, Processor> createProcessorFn
    ) {
        this.contextFactory = contextFactory;
        this.createProcessorFn = createProcessorFn;
    }

    @Override
    public void init(@Nonnull Context context) {
        if (contextFactory.hasLocalSharing()) {
            contextObject = contextFactory.createFn().apply(context.jetInstance());
        }
    }

    @Nonnull
    @Override
    public Collection<? extends Processor> get(int count) {
        return Stream.generate(() -> createProcessorFn.apply(contextFactory, contextObject))
                .limit(count)
                .collect(toList());
    }

    @Override
    public void close(Throwable error) {
        if (contextObject != null) {
            contextFactory.destroyFn().accept(contextObject);
        }
    }

    @Nonnull
    public static <C> ProcessorSupplier supplierWithContext(
            @Nonnull ContextFactory<C> contextFactory,
            @Nonnull BiFunctionEx<ContextFactory<C>, C, Processor> createProcessorFn
    ) {
        return new ProcessorSupplierWithContext<>(contextFactory, createProcessorFn);
    }
}
