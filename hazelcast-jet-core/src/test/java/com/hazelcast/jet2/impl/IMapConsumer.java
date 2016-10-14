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

import com.hazelcast.core.IMap;
import com.hazelcast.jet2.Outbox;
import com.hazelcast.jet2.Processor;
import com.hazelcast.jet2.ProcessorContext;
import com.hazelcast.jet2.ProcessorSupplier;

import javax.annotation.Nonnull;
import java.util.Map;

public class IMapConsumer extends AbstractProcessor {

    private final IMap<Object, Object> map;

    public IMapConsumer(IMap<Object, Object> map) {
        this.map = map;
    }

    @Override
    public void init(@Nonnull Outbox outbox) {
        super.init(outbox);
    }

    @Override
    public boolean process(int ordinal, Object item) {
        Map.Entry<Object, Object> entry = (Map.Entry<Object, Object>) item;
        map.set(entry.getKey(), entry.getValue());
        return true;
    }

    @Override
    public boolean isBlocking() {
        return true;
    }

    public static ProcessorSupplier supplier(String mapName) {
        return new Supplier(mapName);
    }

    private static class Supplier implements ProcessorSupplier {

        private final String name;
        private IMap<Object, Object> map;

        public Supplier(String name) {
            this.name = name;
        }

        @Override
        public void init(ProcessorContext context) {
            map = context.getHazelcastInstance().getMap(name);
        }

        @Override
        public Processor get() {
            return new IMapConsumer(map);
        }
    }
}
