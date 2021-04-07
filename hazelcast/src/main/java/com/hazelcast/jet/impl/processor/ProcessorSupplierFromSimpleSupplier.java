/*
 * Copyright (c) 2008-2021, Hazelcast, Inc. All Rights Reserved.
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

import com.hazelcast.function.SupplierEx;
import com.hazelcast.internal.serialization.SerializableByConvention;
import com.hazelcast.jet.core.Processor;
import com.hazelcast.jet.core.ProcessorSupplier;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.DataSerializable;

import javax.annotation.Nonnull;
import java.io.IOException;
import java.util.Collection;
import java.util.stream.Collectors;
import java.util.stream.Stream;

@SerializableByConvention
public class ProcessorSupplierFromSimpleSupplier implements ProcessorSupplier, DataSerializable {

    private SupplierEx<? extends Processor> simpleSupplier;

    // for deserialization
    @SuppressWarnings("unused")
    public ProcessorSupplierFromSimpleSupplier() {
    }

    public ProcessorSupplierFromSimpleSupplier(SupplierEx<? extends Processor> simpleSupplier) {
        this.simpleSupplier = simpleSupplier;
    }

    @Nonnull @Override
    public Collection<? extends Processor> get(int count) {
        return Stream.generate(simpleSupplier).limit(count).collect(Collectors.toList());
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        out.writeObject(simpleSupplier);
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        simpleSupplier = in.readObject();
    }
}
