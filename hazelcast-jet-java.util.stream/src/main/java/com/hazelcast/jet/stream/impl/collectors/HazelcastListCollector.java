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

package com.hazelcast.jet.stream.impl.collectors;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IList;
import com.hazelcast.jet.api.dag.tap.HazelcastSinkTap;
import com.hazelcast.jet.api.data.tuple.JetTuple2;
import com.hazelcast.jet.io.api.tuple.Tuple;
import com.hazelcast.jet.api.dag.tap.SinkTap;
import com.hazelcast.jet.api.dag.tap.TapType;
import com.hazelcast.jet.stream.Distributed;

import static com.hazelcast.jet.stream.impl.StreamUtil.LIST_PREFIX;
import static com.hazelcast.jet.stream.impl.StreamUtil.randomName;

public class HazelcastListCollector<T> extends AbstractHazelcastCollector<T, IList<T>> {

    private final String listName;

    public HazelcastListCollector() {
        this(randomName(LIST_PREFIX));
    }

    public HazelcastListCollector(String listName) {
        this.listName = listName;
    }

    @Override
    protected IList<T> getTarget(HazelcastInstance instance) {
        return instance.getList(listName);
    }


    protected <U extends T> Distributed.Function<U, Tuple> toTupleMapper() {
        return v -> new JetTuple2<>(0, v);
    }

    @Override
    protected SinkTap getSinkTap() {
        return new HazelcastSinkTap(listName, TapType.HAZELCAST_LIST);
    }

}
