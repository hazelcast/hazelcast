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

import com.hazelcast.function.FunctionEx;
import com.hazelcast.function.SupplierEx;
import com.hazelcast.jet.aggregate.AggregateOperation;
import com.hazelcast.jet.aggregate.AggregateOperation1;
import com.hazelcast.jet.core.JetDataSerializerHook;
import com.hazelcast.jet.core.Processor;
import com.hazelcast.jet.core.ResettableSingletonTraverser;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;

import java.io.IOException;

public final class ProcessorSuppliers {
    public static class AccumulatePSupplier<A, R> implements SupplierEx<Processor>, IdentifiedDataSerializable {
        private AggregateOperation<A, R> aggrOp;

        public AccumulatePSupplier() {
        }

        public AccumulatePSupplier(AggregateOperation<A, R> aggrOp) {
            this.aggrOp = aggrOp;
        }

        @Override
        public Processor getEx() throws Exception {
            return new AggregateP<>(aggrOp.withIdentityFinish());
        }

        @Override
        public void writeData(ObjectDataOutput out) throws IOException {
            out.writeObject(aggrOp);
        }

        @Override
        public void readData(ObjectDataInput in) throws IOException {
            aggrOp = in.readObject();
        }

        @Override
        public int getFactoryId() {
            return JetDataSerializerHook.FACTORY_ID;
        }

        @Override
        public int getClassId() {
            return JetDataSerializerHook.PROCESSORS_ACCUMULATE_P_SUPPLIER;
        }
    }

    public static class CombinePSupplier<A, R> implements SupplierEx<Processor>, IdentifiedDataSerializable {
        private AggregateOperation1<A, A, R> aarAggregateOperation;

        public CombinePSupplier() {
        }

        public CombinePSupplier(AggregateOperation1<A, A, R> aarAggregateOperation) {
            this.aarAggregateOperation = aarAggregateOperation;
        }

        @Override
        public Processor getEx() throws Exception {
            return new AggregateP<>(aarAggregateOperation);
        }

        @Override
        public void writeData(ObjectDataOutput out) throws IOException {
            out.writeObject(aarAggregateOperation);
        }

        @Override
        public void readData(ObjectDataInput in) throws IOException {
            aarAggregateOperation = in.readObject();
        }

        @Override
        public int getFactoryId() {
            return JetDataSerializerHook.FACTORY_ID;
        }

        @Override
        public int getClassId() {
            return JetDataSerializerHook.PROCESSORS_COMBINE_P_SUPPLIER;
        }
    }

    public static class ProcessorMapPSupplier<T, R> implements IdentifiedDataSerializable, SupplierEx<Processor> {
        private FunctionEx<? super T, ? extends R> mapFn;

        public ProcessorMapPSupplier() {
        }

        public ProcessorMapPSupplier(FunctionEx<? super T, ? extends R> mapFn) {
            this.mapFn = mapFn;
        }

        @Override
        public Processor getEx() throws Exception {
            final ResettableSingletonTraverser<R> trav = new ResettableSingletonTraverser<>();
            return new TransformP<T, R>(item -> {
                trav.accept(mapFn.apply(item));
                return trav;
            });
        }

        @Override
        public void writeData(ObjectDataOutput out) throws IOException {
            out.writeObject(mapFn);
        }

        @Override
        public void readData(ObjectDataInput in) throws IOException {
            mapFn = in.readObject();
        }

        @Override
        public int getFactoryId() {
            return JetDataSerializerHook.FACTORY_ID;
        }

        @Override
        public int getClassId() {
            return JetDataSerializerHook.PROCESSOR_MAP_P_SUPPLIER;
        }
    }
}
