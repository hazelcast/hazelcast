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
import com.hazelcast.jet.io.Pair;
import com.hazelcast.jet.runtime.JetPair;
import com.hazelcast.jet.runtime.OutputCollector;

class JetGroupingGate extends GroupingSpliceGate {
    private final Holder<OutputCollector<Pair<Tuple, Tuple>>> outputHolder;

    public JetGroupingGate(
            FlowProcess flowProcess, Splice splice, Holder<OutputCollector<Pair<Tuple, Tuple>>> outputHolder) {
        super(flowProcess, splice);
        this.outputHolder = outputHolder;
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
        JetPair<Tuple, Tuple> pair = new JetPair<>(new Tuple(keyTuple), valueTuple);
        outputHolder.get().collect(pair);
    }

    @Override
    public void complete(Duct previous) {
    }
}
