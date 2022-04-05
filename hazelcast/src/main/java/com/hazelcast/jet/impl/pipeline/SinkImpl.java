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

package com.hazelcast.jet.impl.pipeline;

import com.hazelcast.function.FunctionEx;
import com.hazelcast.jet.core.ProcessorMetaSupplier;
import com.hazelcast.jet.pipeline.Sink;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

public class SinkImpl<T> implements Sink<T> {

    private static final long serialVersionUID = 1L;

    private final String name;
    private final ProcessorMetaSupplier metaSupplier;
    private boolean assignedToStage;

    private final Type type;
    private final FunctionEx<? super T, ?> partitionKeyFunction;

    public SinkImpl(
            @Nonnull String name,
            @Nonnull ProcessorMetaSupplier metaSupplier
    ) {
        this(name, metaSupplier, Type.DEFAULT, null);
    }

    public SinkImpl(
            @Nonnull String name,
            @Nonnull ProcessorMetaSupplier metaSupplier,
            @Nullable FunctionEx<? super T, ?> partitionKeyFn
    ) {
        this(name, metaSupplier, Type.PARTITIONED, partitionKeyFn);
    }

    public SinkImpl(
            @Nonnull String name,
            @Nonnull ProcessorMetaSupplier metaSupplier,
            @Nonnull Type type
    ) {
        this(name, metaSupplier, type, null);
    }

    public SinkImpl(
            @Nonnull String name,
            @Nonnull ProcessorMetaSupplier metaSupplier,
            @Nonnull Type type,
            @Nullable FunctionEx<? super T, ?> partitionKeyFn
    ) {
        if (type.isPartitioned() && partitionKeyFn == null) {
            throw new IllegalArgumentException("Partitioned type " + type + " needs a partition key function");
        }
        if (!type.isPartitioned() && partitionKeyFn != null) {
            throw new IllegalArgumentException("Non partitioned type " + type + " can't have a partition key function");
        }

        this.name = name;
        this.metaSupplier = metaSupplier;
        this.type = type;
        this.partitionKeyFunction = partitionKeyFn;
    }

    @Nonnull
    public ProcessorMetaSupplier metaSupplier() {
        return metaSupplier;
    }

    @Override
    public String name() {
        return name;
    }

    public Type getType() {
        return type;
    }

    public FunctionEx<? super T, ?> partitionKeyFunction() {
        return partitionKeyFunction;
    }

    void onAssignToStage() {
        if (assignedToStage) {
            throw new IllegalStateException("Sink " + name + " was already assigned to a sink stage");
        }
        assignedToStage = true;
    }

    public enum Type {
        DEFAULT(false, false),
        PARTITIONED(true, false),
        DISTRIBUTED_PARTITIONED(true, true),
        TOTAL_PARALLELISM_ONE(false, true);

        boolean partitioned;
        boolean distributed;

        Type(boolean partitioned, boolean distributed) {
            this.partitioned = partitioned;
            this.distributed = distributed;
        }

        public boolean isPartitioned() {
            return partitioned;
        }

        public boolean isDistributed() {
            return distributed;
        }
    }
}
