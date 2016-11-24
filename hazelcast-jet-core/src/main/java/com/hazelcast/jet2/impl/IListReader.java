/*
 * Copyright (c) 2008-2016, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.jet2.impl;

import com.hazelcast.collection.impl.list.ListProxyImpl;
import com.hazelcast.jet2.Outbox;
import com.hazelcast.jet2.Processor;
import com.hazelcast.jet2.ProcessorMetaSupplier;
import com.hazelcast.jet2.ProcessorSupplier;
import com.hazelcast.nio.Address;

import javax.annotation.Nonnull;
import java.util.Iterator;
import java.util.List;

import static java.util.Collections.singletonList;

public final class IListReader extends AbstractProducer {

    private final ListProxyImpl list;
    private Iterator iterator;

    private IListReader(ListProxyImpl list) {
        this.list = list;
    }

    @Override
    public void init(@Nonnull Outbox outbox) {
        super.init(outbox);
        this.iterator = list.iterator();
    }

    @Override
    public boolean complete() {
        while (iterator.hasNext()) {
            emit(iterator.next());
            if (getOutbox().isHighWater()) {
                return false;
            }
        }
        return true;
    }

    @Override
    public boolean isBlocking() {
        return true;
    }

    public static ProcessorMetaSupplier supplier(String listName) {
        return new MetaSupplier(listName);
    }

    private static class MetaSupplier implements ProcessorMetaSupplier {

        static final long serialVersionUID = 1L;
        private final String name;

        private transient Address ownerAddress;

        MetaSupplier(String name) {
            this.name = name;
        }

        @Override
        public void init(Context context) {
            int partitionId = context.getPartitionServce().getPartitionId(name);
            ownerAddress = context.getPartitionServce().getPartitionOwnerOrWait(partitionId);
        }

        @Override
        public ProcessorSupplier get(Address address) {
            if (address.equals(ownerAddress)) {
                return new Supplier(name);
            } else {
                // nothing to read on other nodes
                return (c) -> {
                    assertCountIsOne(c);
                    return singletonList(new AbstractProducer() {
                    });
                };
            }
        }
    }

    private static class Supplier implements ProcessorSupplier {

        static final long serialVersionUID = 1L;

        private final String name;
        private transient ListProxyImpl list;

        Supplier(String name) {
            this.name = name;
        }

        @Override
        public void init(Context context) {
            list = (ListProxyImpl) context.getHazelcastInstance().getList(name);
        }

        @Override
        public List<Processor> get(int count) {
            assertCountIsOne(count);
            return singletonList(new IListReader(list));
        }
    }

    private static void assertCountIsOne(int count) {
        if (count != 1) {
            throw new IllegalArgumentException("count != 1");
        }
    }
}
