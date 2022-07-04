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

package com.hazelcast.jet.impl.connector;

import com.hazelcast.client.impl.clientside.HazelcastClientProxy;
import com.hazelcast.client.impl.proxy.ClientListProxy;
import com.hazelcast.collection.IList;
import com.hazelcast.collection.impl.list.ListProxyImpl;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.internal.serialization.Data;
import com.hazelcast.internal.serialization.SerializationService;
import com.hazelcast.jet.Traverser;
import com.hazelcast.jet.core.AbstractProcessor;
import com.hazelcast.jet.impl.execution.init.Contexts.ProcCtx;

import javax.annotation.Nonnull;
import java.util.List;
import java.util.function.BiFunction;

import static com.hazelcast.client.HazelcastClient.newHazelcastClient;
import static com.hazelcast.jet.Traversers.traverseIterable;
import static com.hazelcast.jet.Traversers.traverseStream;
import static com.hazelcast.jet.impl.util.ImdgUtil.asClientConfig;
import static java.lang.Math.min;
import static java.util.stream.IntStream.rangeClosed;

public final class ReadIListP extends AbstractProcessor {

    static final int FETCH_SIZE = 16384;

    private final String name;
    private final String clientXml;

    private Traverser<Object> traverser;
    private HazelcastInstance client;

    public ReadIListP(String name, String clientXml) {
        this.name = name;
        this.clientXml = clientXml;
    }

    @Override
    public boolean isCooperative() {
        return false;
    }

    @Override
    protected void init(@Nonnull Context context) {
        HazelcastInstance instance;
        SerializationService serializationService;
        if (isRemote()) {
            instance = client = newHazelcastClient(asClientConfig(clientXml));
            serializationService = ((HazelcastClientProxy) instance).getSerializationService();
        } else {
            instance = context.hazelcastInstance();
            serializationService = ((ProcCtx) context).serializationService();
        }
        traverser = createTraverser(instance, name).map(serializationService::toObject);
    }

    @SuppressWarnings("rawtypes")
    private Traverser<Data> createTraverser(HazelcastInstance instance, String name) {
        IList<Data> list = instance.getList(name);
        int size = list.size();

        if (list instanceof ClientListProxy) {
            ClientListProxy proxy = (ClientListProxy) list;
            return createTraverser(size, proxy::dataSubList);
        } else if (list instanceof ListProxyImpl) {
            ListProxyImpl proxy = (ListProxyImpl) list;
            return createTraverser(size, proxy::dataSubList);
        } else {
            throw new RuntimeException("Unexpected list class: " + list.getClass().getName());
        }
    }

    private Traverser<Data> createTraverser(int size,
                                            BiFunction<Integer, Integer, List<Data>> subListSupplier) {
        return size <= FETCH_SIZE ?
                traverseIterable(subListSupplier.apply(0, size)) :
                traverseStream(rangeClosed(0, size / FETCH_SIZE).mapToObj(chunkIndex -> chunkIndex * FETCH_SIZE))
                        .flatMap(start -> traverseIterable(subListSupplier.apply(start, min(start + FETCH_SIZE, size))));
    }

    @Override
    public boolean complete() {
        return emitFromTraverser(traverser);
    }

    @Override
    public void close() {
        if (client != null) {
            client.shutdown();
        }
    }

    private boolean isRemote() {
        return clientXml != null;
    }
}
