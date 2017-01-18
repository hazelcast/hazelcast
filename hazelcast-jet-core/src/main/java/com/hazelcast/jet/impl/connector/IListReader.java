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
import com.hazelcast.jet.Processor;
import com.hazelcast.jet.ProcessorMetaSupplier;
import com.hazelcast.jet.ProcessorSupplier;
import com.hazelcast.jet.Processors.NoopProducer;
import com.hazelcast.nio.Address;
import com.hazelcast.partition.strategy.StringPartitioningStrategy;

import javax.annotation.Nonnull;
import java.util.Iterator;
import java.util.List;

import static com.hazelcast.client.HazelcastClient.newHazelcastClient;
import static java.util.Collections.singletonList;

public final class IListReader extends AbstractProducer {

    private static final int DEFAULT_FETCH_SIZE = 16384;

    private final IList list;
    private final int fetchSize;
    private Iterator iterator;
    private int currentIndex;
    private int size;

    private IListReader(IList list, int fetchSize) {
        this.list = list;
        this.fetchSize = fetchSize;
    }

    @Override
    protected void init(@Nonnull Context context) {
        size = list.size();
        if (fetchSize < size) {
            this.iterator = list.subList(0, fetchSize).iterator();
        } else {
            this.iterator = list.iterator();
        }
    }

    @Override
    public boolean complete() {
        while (hasNext()) {
            if (getOutbox().isHighWater()) {
                return false;
            }
            currentIndex++;
            emit(iterator.next());
        }
        return true;
    }

    private boolean hasNext() {
        return iterator.hasNext() || currentIndex < size && advance();
    }

    private boolean advance() {
        iterator = list.subList(currentIndex, getNextIndex()).iterator();
        return iterator.hasNext();
    }

    private int getNextIndex() {
        int jump = currentIndex + fetchSize;
        return jump >= size ? size : jump;
    }

    @Override
    public boolean isCooperative() {
        return false;
    }

    public static ProcessorMetaSupplier supplier(String listName) {
        return new MetaSupplier(listName, DEFAULT_FETCH_SIZE);
    }

    public static ProcessorMetaSupplier supplier(String listName, int fetchSize) {
        return new MetaSupplier(listName, fetchSize);
    }

    public static ProcessorMetaSupplier supplier(String listName, ClientConfig clientConfig) {
        return new MetaSupplier(listName, clientConfig, DEFAULT_FETCH_SIZE);
    }

    public static ProcessorMetaSupplier supplier(String listName, ClientConfig clientConfig, int fetchSize) {
        return new MetaSupplier(listName, clientConfig, fetchSize);
    }

    private static class MetaSupplier implements ProcessorMetaSupplier {

        static final long serialVersionUID = 1L;
        private final String name;
        private final SerializableClientConfig clientConfig;
        private final int fetchSize;

        private transient Address ownerAddress;

        MetaSupplier(String name, int fetchSize) {
            this(name, null, fetchSize);
        }

        MetaSupplier(String name, ClientConfig clientConfig, int fetchSize) {
            this.name = name;
            this.clientConfig = clientConfig != null ? new SerializableClientConfig(clientConfig) : null;
            this.fetchSize = fetchSize;
        }

        @Override
        public void init(@Nonnull Context context) {
            String partitionKey = StringPartitioningStrategy.getPartitionKey(name);
            ownerAddress = context.jetInstance().getHazelcastInstance().getPartitionService()
                                  .getPartition(partitionKey).getOwner().getAddress();
        }

        @Override
        public ProcessorSupplier get(@Nonnull Address address) {
            if (address.equals(ownerAddress)) {
                return new Supplier(name, clientConfig, fetchSize);
            } else {
                // nothing to read on other nodes
                return (c) -> {
                    assertCountIsOne(c);
                    return singletonList(new NoopProducer());
                };
            }
        }
    }

    private static class Supplier implements ProcessorSupplier {

        static final long serialVersionUID = 1L;

        private final String name;
        private final SerializableClientConfig clientConfig;
        private final int fetchSize;
        private transient IList list;
        private transient HazelcastInstance client;

        Supplier(String name, SerializableClientConfig clientConfig, int fetchSize) {
            this.name = name;
            this.clientConfig = clientConfig;
            this.fetchSize = fetchSize;
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

        @Nonnull
        @Override
        public List<Processor> get(int count) {
            assertCountIsOne(count);
            return singletonList(new IListReader(list, fetchSize));
        }
    }

    private static void assertCountIsOne(int count) {
        if (count != 1) {
            throw new IllegalArgumentException(
                    "Supplier of IListReader asked to create more than one processor instance: " + count);
        }
    }
}
