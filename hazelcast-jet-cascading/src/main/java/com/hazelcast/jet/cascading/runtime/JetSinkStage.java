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
import cascading.flow.StepCounters;
import cascading.flow.stream.duct.Duct;
import cascading.flow.stream.duct.DuctException;
import cascading.flow.stream.element.SinkStage;
import cascading.tap.MultiSinkTap;
import cascading.tap.Tap;
import cascading.tuple.Fields;
import cascading.tuple.TupleEntry;
import com.hazelcast.jet.cascading.tap.InternalJetTap;
import com.hazelcast.jet.cascading.tap.SettableTupleEntryCollector;
import com.hazelcast.jet.Outbox;
import java.io.IOException;

public class JetSinkStage extends SinkStage {

    private final Outbox outbox;
    private SettableTupleEntryCollector collector;

    public JetSinkStage(FlowProcess flowProcess, Tap sink, Outbox outbox) {
        super(flowProcess, sink);
        this.outbox = outbox;
    }

    @Override
    public void prepare() {
        try {
            collector = getCollector(getSink());
            if (getSink().getSinkFields().isAll()) {
                Fields fields = getIncomingScopes().get(0).getIncomingTapFields();
                collector.setFields(fields);
            }
            collector.setOutput(outbox);
        } catch (IOException exception) {
            throw new DuctException("failed opening sink", exception);
        }
    }

    @Override
    public void receive(Duct previous, TupleEntry tupleEntry) {
        try {
            collector.add(tupleEntry);
            flowProcess.increment(StepCounters.Tuples_Written, 1);
            flowProcess.increment(SliceCounters.Tuples_Written, 1);
        } catch (OutOfMemoryError error) {
            handleReThrowableException("out of memory, try increasing task memory allocation", error);
        } catch (CascadingException exception) {
            handleException(exception, tupleEntry);
        } catch (Throwable throwable) {
            handleException(new DuctException("internal error: " + tupleEntry.getTuple().print(), throwable), tupleEntry);
        }
    }

    @Override
    public void cleanup() {
        try {
            if (collector != null) {
                collector.close();
            }

            collector = null;
        } finally {
            super.cleanup();
        }
    }

    private SettableTupleEntryCollector getCollector(Tap sink) throws IOException {
        if (sink instanceof InternalJetTap) {
            return (SettableTupleEntryCollector) sink.openForWrite(flowProcess, outbox);
        } else if (sink instanceof MultiSinkTap) {
            // TODO: just use the collector for the first sink for now - they will all be using the same consumer
            Tap tap = (Tap) ((MultiSinkTap) sink).getChildTaps().next();
            return getCollector(tap);
        }
        throw new UnsupportedOperationException("Unknown tap type " + sink);
    }
}
