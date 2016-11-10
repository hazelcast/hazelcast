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
import cascading.flow.stream.element.MemoryCoGroupClosure;
import cascading.flow.stream.graph.IORole;
import cascading.flow.stream.graph.StreamGraph;
import cascading.pipe.Splice;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntry;
import cascading.tuple.Tuples;
import com.hazelcast.jet2.Inbox;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

class JetCoGroupGate extends JetGroupingSpliceGate implements ProcessorInputSource {

    private Map<Tuple, List<Tuple>>[] keyValues;
    private Set<Tuple> keys;

    private MemoryCoGroupClosure closure;

    JetCoGroupGate(FlowProcess flowProcess, Splice splice) {
        super(flowProcess, splice);
        this.role = IORole.source;
    }

    @Override
    public void prepare() {
        super.prepare();

        keyValues = createKeyValuesArray();
        keys = createKeySet();
        closure = new MemoryCoGroupClosure(flowProcess, splice.getNumSelfJoins(), keyFields, valuesFields);

        if (grouping != null && splice.getJoinDeclaredFields() != null && splice.getJoinDeclaredFields().isNone()) {
            grouping.joinerClosure = closure;
        }
    }

    @Override
    public void initialize() {
        super.initialize();
        initComparators();
    }

    @Override
    public void bind(StreamGraph streamGraph) {
        if (role != IORole.sink) {
            next = getNextFor(streamGraph);
        }
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
    public void complete(Duct previous) {
        if (next != null) {
            super.complete(previous);
        }
    }

    @Override
    public void process(Inbox inbox, int ordinal) throws Throwable {
        Map<Tuple, List<Tuple>> map = keyValues[ordinal];
        for (Object item; (item = inbox.peek()) != null; ) {
            Map.Entry<Tuple, Tuple> pair = (Map.Entry<Tuple, Tuple>) item;
            Tuple key = getDelegatedTuple(pair.getKey());
            Tuple value = pair.getValue();

            keys.add(key);
            map.computeIfAbsent(key, v -> new ArrayList<>()).add(value);
            inbox.remove();
        }
    }

    @Override
    public void complete() {
        next.start(this);
        Collection<Tuple>[] collections = new Collection[keyValues.length];
        Set<Tuple> seenNulls = new HashSet<>();
        for (Iterator<Tuple> keyIterator = keys.iterator(); keyIterator.hasNext(); ) {
            Tuple key = keyIterator.next();
            keyIterator.remove();

            // provides sql like semantics
            if (nullsAreNotEqual && Tuples.frequency(key, null) != 0) {
                if (seenNulls.contains(key)) {
                    continue;
                }

                seenNulls.add(key);

                for (int i = 0; i < keyValues.length; i++) {
                    Collection<Tuple> values = keyValues[i].remove(key);

                    if (values == null) {
                        continue;
                    }

                    for (int j = 0; j < keyValues.length; j++) {
                        collections[j] = Collections.emptyList();
                    }

                    collections[i] = values;

                    push(collections, key);
                }
            } else {
                // drain the keys and keyValues collections to preserve memory
                for (int i = 0; i < keyValues.length; i++) {
                    collections[i] = keyValues[i].remove(key);

                    if (collections[i] == null) {
                        collections[i] = Collections.emptyList();
                    }
                }

                push(collections, key);
            }
        }

        keys = createKeySet();
        keyValues = createKeyValuesArray();

        complete(this);
    }

    private Map<Tuple, List<Tuple>>[] createKeyValuesArray() {
        @SuppressWarnings("unchecked")
        Map<Tuple, List<Tuple>>[] valueMap = new Map[getNumDeclaredIncomingBranches()];

        for (int i = 0; i < getNumDeclaredIncomingBranches(); i++) {
            valueMap[i] = new HashMap<>();
        }

        return valueMap;
    }

    private void push(Collection<Tuple>[] collections, Tuple keysTuple) {
        closure.reset(collections);
        keyEntry.setTuple(closure.getGroupTuple(keysTuple));
        // create Closure type here
        tupleEntryIterator.reset(splice.getJoiner().getIterator(closure));
        next.receive(this, grouping);
    }

    protected Set<Tuple> createKeySet() {
        return new TreeSet<>(getKeyComparator());
    }

}
