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

import com.hazelcast.core.IList;
import com.hazelcast.jet2.AbstractProcessor;
import com.hazelcast.jet2.Inbox;
import com.hazelcast.jet2.Outbox;
import com.hazelcast.jet2.Processor;
import com.hazelcast.jet2.ProcessorSupplier;

import javax.annotation.Nonnull;
import java.util.List;
import java.util.stream.Stream;

import static java.util.stream.Collectors.toList;

public final class IListWriter extends AbstractProcessor {

    private IList list;

    private IListWriter(IList list) {
        this.list = list;
    }

    @Override
    public void init(@Nonnull Outbox outbox) {
        super.init(outbox);
    }

    @Override
    public void process(int ordinal, Inbox inbox) {
        inbox.drainTo(list);
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
        private transient IList list;

        Supplier(String name) {
            this.name = name;
        }

        @Override
        public void init(Context context) {
            list = context.getHazelcastInstance().getList(name);
        }

        @Override
        public List<Processor> get(int count) {
            return Stream.generate(() -> new IListWriter(list)).limit(count).collect(toList());
        }
    }
}
