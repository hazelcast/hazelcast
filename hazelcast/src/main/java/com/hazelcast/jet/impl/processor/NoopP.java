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

import com.hazelcast.function.ConsumerEx;
import com.hazelcast.function.SupplierEx;
import com.hazelcast.jet.core.Inbox;
import com.hazelcast.jet.core.Outbox;
import com.hazelcast.jet.core.Processor;
import com.hazelcast.jet.core.Watermark;
import com.hazelcast.jet.core.processor.Processors;
import com.hazelcast.jet.impl.execution.init.JetInitDataSerializerHook;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;

import javax.annotation.Nonnull;
import java.io.IOException;

/**
 * A no-operation processor. See {@link Processors#noopP()}.
 */
public class NoopP implements Processor {
    private Outbox outbox;

    @Override
    public void init(@Nonnull Outbox outbox, @Nonnull Context context) throws Exception {
        this.outbox = outbox;
    }

    @Override
    public void process(int ordinal, @Nonnull Inbox inbox) {
        inbox.drain(ConsumerEx.noop());
    }

    @Override
    public boolean tryProcessWatermark(@Nonnull Watermark watermark) {
        return outbox.offer(watermark);
    }

    @Override
    public void restoreFromSnapshot(@Nonnull Inbox inbox) {
        inbox.drain(ConsumerEx.noop());
    }

    @Override
    public boolean closeIsCooperative() {
        return true;
    }

    public static final class NoopPSupplier implements SupplierEx<Processor>, IdentifiedDataSerializable {

        @Override
        public Processor getEx() throws Exception {
            return new NoopP();
        }

        @Override
        public int getFactoryId() {
            return JetInitDataSerializerHook.FACTORY_ID;
        }

        @Override
        public int getClassId() {
            return JetInitDataSerializerHook.NOOP_PROCESSOR_SUPPLIER;
        }

        @Override
        public void writeData(ObjectDataOutput out) throws IOException {
        }

        @Override
        public void readData(ObjectDataInput in) throws IOException {
        }
    }
}
