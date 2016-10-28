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
import com.hazelcast.jet2.ProcessorSupplierContext;
import com.hazelcast.jet2.ProcessorSupplier;
import java.util.Iterator;
import javax.annotation.Nonnull;

public class IListReader extends AbstractProducer {

    private final ListProxyImpl list;
    private Iterator iterator;


    protected IListReader(ListProxyImpl list) {
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

    public static ProcessorSupplier supplier(String listName) {
        return new Supplier(listName);
    }

    private static class Supplier implements ProcessorSupplier {

        static final long serialVersionUID = 1L;

        private final String name;
        private transient ListProxyImpl list;

        public Supplier(String name) {
            this.name = name;
        }

        @Override
        public void init(ProcessorSupplierContext context) {
            list = (ListProxyImpl) context.getHazelcastInstance().getList(name);
        }

        @Override
        public Processor get() {
            return new IListReader(list);
        }
    }
}
