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

import cascading.CascadingException;
import cascading.flow.FlowProcess;
import cascading.flow.SliceCounters;
import cascading.flow.planner.Scope;
import cascading.flow.stream.duct.Duct;
import cascading.flow.stream.duct.DuctException;
import cascading.flow.stream.element.SpliceGate;
import cascading.flow.stream.graph.IORole;
import cascading.flow.stream.graph.StreamGraph;
import cascading.pipe.Splice;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntry;
import cascading.tuple.io.ValueTuple;
import cascading.util.Util;
import com.hazelcast.jet.Inbox;
import com.hazelcast.jet.Outbox;

import java.util.Map;

import static com.hazelcast.jet.Util.entry;

public class JetMergeGate extends SpliceGate<TupleEntry, TupleEntry> implements ProcessorInputSource {

    private Outbox outbox;
    private TupleEntry valueEntry;

    public JetMergeGate(FlowProcess flowProcess, Splice splice) {
        super(flowProcess, splice, IORole.source);
    }

    public JetMergeGate(FlowProcess flowProcess, Splice splice, Outbox outbox) {
        super(flowProcess, splice, IORole.sink);
        this.outbox = outbox;
    }

    @Override
    public void initialize() {
        super.initialize();
    }

    @Override
    public void prepare() {
        super.prepare();

    }

    @Override
    public void start(Duct previous) {
        if (next != null) {
            super.start(previous);
        }
    }

    @Override
    public void bind(StreamGraph streamGraph) {
        if (role != IORole.sink) {
            next = getNextFor(streamGraph);
        }
    }

    @Override
    public void receive(Duct previous, TupleEntry incomingEntry) {
        try {
            boolean accepted = outbox.offer(entry(incomingEntry.getTupleCopy(), ValueTuple.NULL));
            assert accepted : "Outbox refused item";
            flowProcess.increment(SliceCounters.Tuples_Written, 1);
        } catch (OutOfMemoryError error) {
            handleReThrowableException("out of memory, try increasing task memory allocation", error);
        } catch (CascadingException exception) {
            handleException(exception, incomingEntry);
        } catch (Throwable throwable) {
            handleException(new DuctException("internal error: " + incomingEntry.getTuple().print(), throwable),
                    incomingEntry);
        }
    }

    @Override
    public void before() {
        Scope outgoingScope = Util.getFirst(outgoingScopes);
        valueEntry = new TupleEntry(outgoingScope.getOutValuesFields(), true);
        start(this);
    }

    @Override
    public void process(Inbox inbox, int ordinal) throws Throwable {
        for (Object item; (item = inbox.peek()) != null; ) {
            Map.Entry pair = (Map.Entry) item;
            Object key = pair.getKey();
            if (key instanceof Tuple) {
                valueEntry.setTuple((Tuple) key);
            } else {
                valueEntry.setTuple(new Tuple(key));
            }
            next.receive(this, valueEntry);
            inbox.remove();
        }
    }

    @Override
    public void complete() {
        complete(this);
    }

    @Override
    public void complete(Duct previous) {
        if (next != null) {
            super.complete(previous);
        }
    }
}
