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

import cascading.CascadingException;
import cascading.flow.FlowProcess;
import cascading.flow.SliceCounters;
import cascading.flow.planner.Scope;
import cascading.flow.stream.duct.Duct;
import cascading.flow.stream.duct.DuctException;
import cascading.flow.stream.element.BoundaryStage;
import cascading.flow.stream.graph.IORole;
import cascading.flow.stream.graph.StreamGraph;
import cascading.pipe.Boundary;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntry;
import cascading.util.Util;
import com.hazelcast.jet2.Inbox;
import com.hazelcast.jet2.Outbox;

import java.util.Map;

public class JetBoundaryStage extends BoundaryStage<TupleEntry, TupleEntry> implements ProcessorInputSource {

    private Outbox outbox;
    private TupleEntry valueEntry;

    public JetBoundaryStage(FlowProcess flowProcess, Boundary boundary) {
        super(flowProcess, boundary, IORole.source);
    }

    public JetBoundaryStage(FlowProcess flowProcess, Boundary boundary, Outbox outbox) {
        super(flowProcess, boundary, IORole.sink);
        this.outbox = outbox;
    }

    @Override
    public void initialize() {
        super.initialize();

        Scope outgoingScope = Util.getFirst(outgoingScopes);
        if (role == IORole.source) {
            valueEntry = new TupleEntry(outgoingScope.getIncomingFunctionPassThroughFields(), true);
        }
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
            Tuple tuple = incomingEntry.getTupleCopy();
            outbox.add(tuple);
            flowProcess.increment(SliceCounters.Tuples_Written, 1);
        } catch (OutOfMemoryError error) {
            handleReThrowableException("out of memory, try increasing task memory allocation", error);
        } catch (CascadingException exception) {
            handleException(exception, incomingEntry);
        } catch (Throwable throwable) {
            handleException(new DuctException("internal error: " + incomingEntry.getTuple().print(), throwable), incomingEntry);
        }
    }

    @Override
    public void before() {
        start(this);
    }

    @Override
    public void process(Inbox inbox, int ordinal) throws Throwable {
        for (Object item; (item = inbox.peek()) != null; ) {
            if (item instanceof Tuple) {
                valueEntry.setTuple((Tuple) item);
            } else {
                Map.Entry entry = (Map.Entry) item;
                valueEntry.setTuple(new Tuple(entry.getValue()));
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
