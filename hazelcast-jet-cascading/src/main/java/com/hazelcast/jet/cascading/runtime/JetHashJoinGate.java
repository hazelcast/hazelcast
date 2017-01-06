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
import cascading.flow.stream.element.MemorySpliceGate;
import cascading.flow.stream.graph.StreamGraph;
import cascading.pipe.HashJoin;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntry;
import cascading.tuple.util.TupleBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

import static cascading.tuple.util.TupleViews.createNarrow;

/**
 *
 */
public class JetHashJoinGate extends MemorySpliceGate {
    private static final Logger LOG = LoggerFactory.getLogger(JetHashJoinGate.class);

    private Collection<Tuple>[] collections;
    private List<Tuple> streamedCollection;

    public JetHashJoinGate(FlowProcess flowProcess, HashJoin join) {
        super(flowProcess, join);
    }

    @Override
    public void bind(StreamGraph streamGraph) {
        super.bind(streamGraph);

        // the number of paths incoming
        count.set(numIncomingEventingPaths);
    }

    @Override
    public void prepare() {
        super.prepare();

        // placeholder in collection
        streamedCollection = new ArrayList<>(Arrays.asList(new Tuple()));
        collections = new Collection[getNumDeclaredIncomingBranches()];
        collections[0] = streamedCollection;

        if (nullsAreNotEqual) {
            LOG.warn("HashJoin does not fully support key comparators where null values are not treated equal");
        }
    }

    @Override
    protected TupleBuilder createDefaultNarrowBuilder(final Fields incomingFields, final Fields narrowFields) {
        final int[] pos;
        //incomingFields.getPos() is not thread-safe
        synchronized (incomingFields) {
            pos = incomingFields.getPos(narrowFields);
        }
        return new TupleBuilder() {
            @Override
            public Tuple makeResult(Tuple input, Tuple output) {
                return createNarrow(pos, input);
            }
        };
    }

    @Override
    public void receive(Duct previous, TupleEntry incomingEntry) {
        int pos = ordinalMap.get(previous);

        Tuple incomingTuple = incomingEntry.getTupleCopy();
        // view in incomingTuple
        Tuple keyTuple = keyBuilder[pos].makeResult(incomingTuple, null);
        keyTuple = getDelegatedTuple(keyTuple);

        if (pos != 0) {
            keys.add(keyTuple);
            keyValues[pos].get(keyTuple).add(incomingTuple);
            return;
        }

        keys.remove(keyTuple);
        streamedCollection.set(0, incomingTuple); // no need to copy, temp setting
        performJoinWith(keyTuple);
    }

    private void performJoinWith(Tuple keyTuple) {
        // never replace the first array, pos == 0
        for (int i = 1; i < keyValues.length; i++) {
            // if key does not exist, #get will create an empty array list,
            // and store the key, which is not a copy
            if (keyValues[i].containsKey(keyTuple)) {
                collections[i] = keyValues[i].get(keyTuple);
            } else {
                collections[i] = Collections.EMPTY_LIST;
            }
        }

        closure.reset(collections);

        keyEntry.setTuple(keyTuple);
        tupleEntryIterator.reset(splice.getJoiner().getIterator(closure));

        next.receive(this, grouping);
    }

    @Override
    public void complete(Duct previous) {
        if (count.decrementAndGet() != 0) {
            return;
        }

        collections[0] = Collections.EMPTY_LIST;

        keys.forEach(this::performJoinWith);

        keys = createKeySet();
        keyValues = createKeyValuesArray();

        super.complete(previous);
    }

    @Override
    protected boolean isBlockingStreamed() {
        return false;
    }
}
