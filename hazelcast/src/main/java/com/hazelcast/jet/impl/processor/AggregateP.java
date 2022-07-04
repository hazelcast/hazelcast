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

package com.hazelcast.jet.impl.processor;

import com.hazelcast.jet.aggregate.AggregateOperation;

import javax.annotation.Nonnull;

import static java.util.Collections.nCopies;

/**
 * Batch processor that computes the supplied aggregate operation. The
 * items may originate from one or more inbound edges. The supplied
 * aggregate operation must have as many accumulation functions as there
 * are inbound edges.
 */
public class AggregateP<A, R> extends GroupP<String, A, R, R> {

    private static final String CONSTANT_KEY = "ALL";

    public AggregateP(@Nonnull AggregateOperation<A, R> aggrOp) {
        super(nCopies(aggrOp.arity(), t -> CONSTANT_KEY), aggrOp, (k, r) -> r);
        keyToAcc.put(CONSTANT_KEY, aggrOp.createFn().get());
    }
}
