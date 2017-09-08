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

package com.hazelcast.jet.impl.connector;

import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IList;
import com.hazelcast.jet.AbstractProcessor;
import com.hazelcast.jet.Processor;
import com.hazelcast.jet.ProcessorMetaSupplier;
import com.hazelcast.jet.ProcessorSupplier;
import com.hazelcast.jet.processor.Processors;
import com.hazelcast.jet.Traverser;
import com.hazelcast.jet.function.DistributedFunction;
import com.hazelcast.nio.Address;
import com.hazelcast.partition.strategy.StringPartitioningStrategy;

import javax.annotation.Nonnull;
import java.util.List;

import static com.hazelcast.client.HazelcastClient.newHazelcastClient;
import static com.hazelcast.jet.Traversers.traverseIterable;
import static com.hazelcast.jet.Traversers.traverseStream;
import static java.lang.Math.min;
import static java.util.Collections.singletonList;
import static java.util.stream.IntStream.rangeClosed;

public final class ReadIListP extends AbstractProcessor {

    private static final int FETCH_SIZE = 16384;

    private final Traverser<Object> traverser;

    ReadIListP(List<Object> list) {
        final int size = list.size();
        traverser = size <= FETCH_SIZE
                ? traverseIterable(list)
                : traverseStream(rangeClosed(0, size / FETCH_SIZE).mapToObj(chunkIndex -> chunkIndex * FETCH_SIZE))
                    .flatMap(start -> traverseIterable(list.subList(start, min(start + FETCH_SIZE, size))));
    }

    @Override
    public boolean complete() {
        return emitFromTraverser(traverser);
    }

    @Override
    public boolean isCooperative() {
        return false;
    }

    public static ProcessorMetaSupplier supplier(String listName) {
        return new MetaSupplier(listName);
    }

    public static ProcessorMetaSupplier supplier(String listName, ClientConfig clientConfig) {
        return new MetaSupplier(listName, clientConfig);
    }

    private static class MetaSupplier implements ProcessorMetaSupplier {

        static final long serialVersionUID = 1L;
        private final String name;
        private final SerializableClientConfig clientConfig;

        private transient Address ownerAddress;

        MetaSupplier(String name) {
            this(name, null);
        }

        MetaSupplier(String name, ClientConfig clientConfig) {
            this.name = name;
            this.clientConfig = clientConfig != null ? new SerializableClientConfig(clientConfig) : null;
        }

        @Override
        public void init(@Nonnull Context context) {
            String partitionKey = StringPartitioningStrategy.getPartitionKey(name);
            ownerAddress = context.jetInstance().getHazelcastInstance().getPartitionService()
                                  .getPartition(partitionKey).getOwner().getAddress();
        }

        @Override @Nonnull
        public DistributedFunction<Address, ProcessorSupplier> get(@Nonnull List<Address> addresses) {
            return address -> {
                if (address.equals(ownerAddress)) {
                    return new Supplier(name, clientConfig);
                }
                // return empty producer on all other nodes
                return c -> {
                    assertCountIsOne(c);
                    return singletonList(Processors.noop().get());
                };
            };
        }
    }

    private static class Supplier implements ProcessorSupplier {

        static final long serialVersionUID = 1L;

        private final String name;
        private final SerializableClientConfig clientConfig;
        private transient IList<Object> list;
        private transient HazelcastInstance client;

        Supplier(String name, SerializableClientConfig clientConfig) {
            this.name = name;
            this.clientConfig = clientConfig;
        }

        @Override
        public void init(@Nonnull Context context) {
            HazelcastInstance instance;
            if (isRemote()) {
                instance = client = newHazelcastClient(clientConfig.asClientConfig());
            } else {
                instance = context.jetInstance().getHazelcastInstance();
            }
            list = instance.getList(name);
        }

        private boolean isRemote() {
            return clientConfig != null;
        }

        @Override
        public void complete(Throwable error) {
            if (client != null) {
                client.shutdown();
            }
        }

        @Override @Nonnull
        public List<Processor> get(int count) {
            assertCountIsOne(count);
            return singletonList(new ReadIListP(list));
        }
    }

    private static void assertCountIsOne(int count) {
        if (count != 1) {
            throw new IllegalArgumentException(
                    "Supplier of IListReader asked to create more than one processor instance: " + count);
        }
    }
}
