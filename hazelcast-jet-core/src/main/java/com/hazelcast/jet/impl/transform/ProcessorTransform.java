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

package com.hazelcast.jet.impl.transform;

import com.hazelcast.jet.core.Processor;
import com.hazelcast.jet.function.DistributedSupplier;

/**
 * A unary transform constructed directly from a provided Core API
 * processor supplier.
 */
public class ProcessorTransform<E, R> implements UnaryTransform<E, R> {
    public final DistributedSupplier<Processor> procSupplier;
    private final String name;

    public ProcessorTransform(String name, DistributedSupplier<Processor> procSupplier) {
        this.name = name;
        this.procSupplier = procSupplier;
    }

    @Override
    public String name() {
        return name;
    }
}
