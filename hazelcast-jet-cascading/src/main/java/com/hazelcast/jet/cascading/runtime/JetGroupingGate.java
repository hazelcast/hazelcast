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

package com.hazelcast.jet.cascading.runtime;

import cascading.flow.FlowProcess;
import cascading.flow.stream.duct.Duct;
import cascading.flow.stream.element.GroupingSpliceGate;
import cascading.flow.stream.graph.StreamGraph;
import cascading.pipe.Splice;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntry;
import com.hazelcast.jet2.Outbox;

import java.util.AbstractMap.SimpleImmutableEntry;

class JetGroupingGate extends GroupingSpliceGate {
    private final Outbox outbox;

    JetGroupingGate(FlowProcess flowProcess, Splice splice, Outbox outbox) {
        super(flowProcess, splice);
        this.outbox = outbox;
    }

    @Override
    public void start(Duct previous) {

    }

    @Override
    public void bind(StreamGraph streamGraph) {
        setOrdinalMap(streamGraph);
    }

    @Override
    public void receive(Duct previous, TupleEntry incomingEntry) {
        // view over the incoming tuple
        Integer pos = ordinalMap.get(previous);

        Tuple keyTuple = keyBuilder[pos].makeResult(incomingEntry.getTuple(), null);
        Tuple valueTuple = incomingEntry.getTupleCopy();
        SimpleImmutableEntry<Tuple, Tuple> pair = new SimpleImmutableEntry<>(new Tuple(keyTuple), valueTuple);
        outbox.add(pair);
    }

    @Override
    public void complete(Duct previous) {
    }
}
