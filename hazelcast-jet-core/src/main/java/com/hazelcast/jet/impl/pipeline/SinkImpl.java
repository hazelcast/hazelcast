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

package com.hazelcast.jet.impl.pipeline;

import com.hazelcast.jet.core.ProcessorMetaSupplier;
import com.hazelcast.jet.pipeline.Sink;

import javax.annotation.Nonnull;
import java.util.concurrent.atomic.AtomicBoolean;

public class SinkImpl<T> implements Sink<T> {

    private final String name;
    private ProcessorMetaSupplier metaSupplier;
    private final AtomicBoolean isAssignedToStage = new AtomicBoolean();

    public SinkImpl(@Nonnull String name, @Nonnull ProcessorMetaSupplier metaSupplier) {
        this.name = name;
        this.metaSupplier = metaSupplier;
    }

    @Nonnull
    public ProcessorMetaSupplier metaSupplier() {
        return metaSupplier;
    }

    @Override
    public String name() {
        return name;
    }

    void onAssignToStage() {
        if (isAssignedToStage.getAndSet(true)) {
            throw new IllegalStateException("Sink " + name + " was already assigned to a sink stage");
        }
    }
}
