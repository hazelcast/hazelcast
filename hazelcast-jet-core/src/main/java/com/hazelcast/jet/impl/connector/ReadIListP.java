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

package com.hazelcast.jet.impl.connector;

import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IList;
import com.hazelcast.jet.Traverser;
import com.hazelcast.jet.core.AbstractProcessor;
import com.hazelcast.jet.core.ProcessorMetaSupplier;
import com.hazelcast.jet.core.ProcessorSupplier;

import javax.annotation.Nonnull;

import static com.hazelcast.client.HazelcastClient.newHazelcastClient;
import static com.hazelcast.jet.Traversers.traverseIterable;
import static com.hazelcast.jet.Traversers.traverseStream;
import static com.hazelcast.jet.core.ProcessorMetaSupplier.forceTotalParallelismOne;
import static com.hazelcast.jet.impl.util.Util.asClientConfig;
import static com.hazelcast.jet.impl.util.Util.asXmlString;
import static java.lang.Math.min;
import static java.util.stream.IntStream.rangeClosed;

public final class ReadIListP extends AbstractProcessor {

    static final int FETCH_SIZE = 16384;
    private final String name;
    private final String clientXml;

    private Traverser<Object> traverser;
    private HazelcastInstance client;

    ReadIListP(String name, String clientXml) {
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
        if (isRemote()) {
            instance = client = newHazelcastClient(asClientConfig(clientXml));
        } else {
            instance = context.jetInstance().getHazelcastInstance();
        }
        IList<Object> list = instance.getList(name);
        final int size = list.size();
        traverser = size <= FETCH_SIZE ?
                traverseIterable(list)
                :
                traverseStream(rangeClosed(0, size / FETCH_SIZE).mapToObj(chunkIndex -> chunkIndex * FETCH_SIZE))
                        .flatMap(start -> traverseIterable(list.subList(start, min(start + FETCH_SIZE, size))));
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

    public static ProcessorMetaSupplier metaSupplier(String listName, ClientConfig clientConfig) {
        String clientXml = asXmlString(clientConfig);
        return forceTotalParallelismOne(ProcessorSupplier.of(() -> new ReadIListP(listName, clientXml)), listName);
    }
}
