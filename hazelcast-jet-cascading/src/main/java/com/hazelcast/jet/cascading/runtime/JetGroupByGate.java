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

package com.hazelcast.jet.cascading.runtime;

import cascading.flow.FlowProcess;
import cascading.flow.stream.duct.Duct;
import cascading.flow.stream.graph.IORole;
import cascading.flow.stream.graph.StreamGraph;
import cascading.pipe.Splice;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntry;
import com.hazelcast.jet.Inbox;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

class JetGroupByGate extends JetGroupingSpliceGate implements ProcessorInputSource {

    private Map<Tuple, List<Tuple>> valueMap;

    JetGroupByGate(FlowProcess flowProcess, Splice splice) {
        super(flowProcess, splice);
        this.role = IORole.source;
    }

    @Override
    public void prepare() {
        super.prepare();

        valueMap = initValueMap();
    }

    @Override
    public void initialize() {
        super.initialize();
        initComparators();
    }

    @Override
    public void bind(StreamGraph streamGraph) {
        next = getNextFor(streamGraph);
    }

    @Override
    public void start(Duct previous) {
        // chained below in #complete()
    }

    @Override
    public void receive(Duct previous, TupleEntry incomingEntry) {
        throw new UnsupportedOperationException("receive is not supported");
    }

    @Override
    public void process(Inbox inbox, int ordinal) throws Throwable {
        for (Object item; (item = inbox.peek()) != null; ) {
            Map.Entry<Tuple, Tuple> entry = (Map.Entry) item;
            List<Tuple> values = valueMap.computeIfAbsent(entry.getKey(), v -> new ArrayList<>());
            values.add(entry.getValue());
            inbox.remove();
        }
    }

    @Override
    public void complete() {
        next.start(this);

        // drain the keys and keyValues collections to preserve memory
        Iterator<Map.Entry<Tuple, List<Tuple>>> iterator = valueMap.entrySet().iterator();
        while (iterator.hasNext()) {
            Map.Entry<Tuple, List<Tuple>> entry = iterator.next();
            iterator.remove();
            keyEntry.setTuple(entry.getKey());
            List<Tuple> tuples = entry.getValue();
            if (valueComparators != null) {
                Collections.sort(tuples, valueComparators[0]);
            }

            tupleEntryIterator.reset(tuples.iterator());
            next.receive(this, grouping);
        }

        next.complete(this);

        valueMap = initValueMap();
    }

    private Map<Tuple, List<Tuple>> initValueMap() {
        return new TreeMap<>(getKeyComparator());
    }

}
