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
import com.hazelcast.jet.Inbox;
import com.hazelcast.jet.Processor;
import com.hazelcast.jet.ProcessorSupplier;

import javax.annotation.Nonnull;
import java.util.List;
import java.util.stream.Stream;

import static com.hazelcast.client.HazelcastClient.newHazelcastClient;
import static java.util.stream.Collectors.toList;

public final class IListWriter implements Processor {

    private IList list;

    private IListWriter(IList list) {
        this.list = list;
    }

    @Override
    public void process(int ordinal, @Nonnull Inbox inbox) {
        inbox.drainTo(list);
    }

    @Override
    public boolean isCooperative() {
        return false;
    }

    public static ProcessorSupplier supplier(String listName) {
        return new Supplier(listName);
    }

    public static ProcessorSupplier supplier(String listName, ClientConfig clientConfig) {
        return new Supplier(listName, clientConfig);
    }

    private static class Supplier implements ProcessorSupplier {

        static final long serialVersionUID = 1L;

        private final String name;
        private final SerializableClientConfig clientConfig;
        private transient IList list;
        private transient HazelcastInstance client;

        Supplier(String name) {
            this(name, null);
        }

        Supplier(String name, ClientConfig clientConfig) {
            this.name = name;
            this.clientConfig = clientConfig != null ? new SerializableClientConfig(clientConfig) : null;
        }

        private boolean isRemote() {
            return clientConfig != null;
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

        @Override
        public void complete(Throwable error) {
            if (client != null) {
                client.shutdown();
            }
        }

        @Nonnull
        @Override
        public List<Processor> get(int count) {
            return Stream.generate(() -> new IListWriter(list)).limit(count).collect(toList());
        }
    }
}
