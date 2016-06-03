/*
 * Copyright (c) 2008-2016, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.jet.stream.impl;

import com.hazelcast.jet.data.tuple.JetTuple2;
import com.hazelcast.jet.io.tuple.Tuple;
import com.hazelcast.jet.stream.Distributed;
import com.hazelcast.jet.stream.impl.pipeline.StreamContext;

public abstract class AbstractIntermediatePipeline<E_IN, E_OUT> extends AbstractPipeline<E_OUT> {

    protected final Pipeline<E_IN> upstream;

    public AbstractIntermediatePipeline(StreamContext context, boolean isOrdered, Pipeline<E_IN> upstream) {
        super(context, isOrdered);
        this.upstream = upstream;
    }

    protected Distributed.Function<E_IN, Tuple> toTupleMapper() {
        return v -> new JetTuple2<>(v, v);
    }

}
