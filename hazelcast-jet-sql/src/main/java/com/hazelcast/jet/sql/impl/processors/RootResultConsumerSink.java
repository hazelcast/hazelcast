/*
 * Copyright (c) 2008-2020, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.jet.sql.impl.processors;

import com.hazelcast.cluster.Address;
import com.hazelcast.instance.impl.HazelcastInstanceImpl;
import com.hazelcast.jet.core.Inbox;
import com.hazelcast.jet.core.Outbox;
import com.hazelcast.jet.core.Processor;
import com.hazelcast.jet.core.ProcessorMetaSupplier;
import com.hazelcast.jet.core.ProcessorSupplier;
import com.hazelcast.jet.core.Watermark;
import com.hazelcast.jet.sql.impl.JetQueryResultProducer;
import com.hazelcast.jet.sql.impl.JetSqlCoreBackendImpl;
import com.hazelcast.sql.impl.JetSqlCoreBackend;
import com.hazelcast.sql.impl.QueryId;

import javax.annotation.Nonnull;

import static com.hazelcast.jet.core.ProcessorMetaSupplier.forceTotalParallelismOne;

public final class RootResultConsumerSink implements Processor {

    private final String queryId;
    private JetQueryResultProducer rootResultConsumer;

    private RootResultConsumerSink(String queryId) {
        this.queryId = queryId;
    }

    @Override
    public void init(@Nonnull Outbox outbox, @Nonnull Context context) {
        HazelcastInstanceImpl hzInst = (HazelcastInstanceImpl) context.jetInstance().getHazelcastInstance();
        JetSqlCoreBackendImpl jetSqlCoreBackend = hzInst.node.nodeEngine.getService(JetSqlCoreBackend.SERVICE_NAME);
        rootResultConsumer = jetSqlCoreBackend.getResultConsumerRegistry().remove(queryId);
        assert rootResultConsumer != null;
    }

    @Override
    public boolean tryProcess() {
        rootResultConsumer.check();
        return true;
    }

    @Override
    public void process(int ordinal, @Nonnull Inbox inbox) {
        rootResultConsumer.consume(inbox);
    }

    @Override
    public boolean complete() {
        rootResultConsumer.done();
        return true;
    }

    @Override
    public boolean tryProcessWatermark(@Nonnull Watermark watermark) {
        return true;
    }

    public static ProcessorMetaSupplier rootResultConsumerSink(Address initiatorAddress, QueryId queryId) {
        String queryIdStr = queryId.toString();
        ProcessorSupplier pSupplier = ProcessorSupplier.of(() -> new RootResultConsumerSink(queryIdStr));
        return forceTotalParallelismOne(pSupplier, initiatorAddress);
    }
}
