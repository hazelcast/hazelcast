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

package com.hazelcast.jet.cascading;

import cascading.flow.FlowProcess;
import cascading.scheme.Scheme;
import cascading.scheme.SinkCall;
import cascading.scheme.SourceCall;
import cascading.tap.Tap;
import cascading.tuple.Fields;
import cascading.tuple.TupleEntry;
import com.hazelcast.jet.config.JetConfig;

import java.io.IOException;
import java.util.AbstractMap.SimpleImmutableEntry;
import java.util.Iterator;
import java.util.Map.Entry;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;

import static com.hazelcast.jet.impl.util.ExceptionUtil.rethrow;

public class TextLine extends Scheme<JetConfig, Iterator<Entry>, Consumer<Entry>, Void, Integer> {

    public static final Fields DEFAULT_SOURCE_FIELDS = new Fields("num", "line").applyTypes(Long.TYPE, String.class);

    //TODO: only works with same JVM.
    private static final AtomicInteger COUNTER = new AtomicInteger(0);

    public TextLine() {
        super(DEFAULT_SOURCE_FIELDS, Fields.ALL);
    }

    public TextLine(Fields sourceFields) {
        super(sourceFields);
    }

    public TextLine(Fields sourceFields, Fields sinkFields) {
        super(sourceFields, sinkFields);
    }

    @Override
    public boolean source(
            FlowProcess<? extends JetConfig> flowProcess, SourceCall<Void, Iterator<Entry>> sourceCall
    ) throws IOException {
        Iterator<Entry> iterator = sourceCall.getInput();
        if (!iterator.hasNext()) {
            return false;
        }
        Entry entry = iterator.next();
        if (getSourceFields().size() == 1) {
            sourceCall.getIncomingEntry().setObject(0, entry.getValue());
        } else {
            sourceCall.getIncomingEntry().setObject(0, entry.getKey());
            sourceCall.getIncomingEntry().setObject(1, entry.getValue());
        }
        return true;
    }

    @Override
    public void sink(FlowProcess<? extends JetConfig> flowProcess, SinkCall<Integer, Consumer<Entry>> sinkCall)
    throws IOException {
        Consumer<Entry> output = sinkCall.getOutput();
        TupleEntry outgoing = sinkCall.getOutgoingEntry();
        try {
            output.accept(new SimpleImmutableEntry<>(COUNTER.getAndIncrement(), outgoing.getTuple().toString()));
        } catch (Exception e) {
            throw rethrow(e);
        }
    }

    @Override
    public void sourceConfInit(FlowProcess<? extends JetConfig> flowProcess,
                               Tap<JetConfig, Iterator<Entry>, Consumer<Entry>> tap, JetConfig conf) {
    }

    @Override
    public void sinkConfInit(
            FlowProcess<? extends JetConfig> flowProcess,
            Tap<JetConfig, Iterator<Entry>, Consumer<Entry>> tap,
            JetConfig conf
    ) {
    }

    @Override
    public void sourcePrepare(FlowProcess<? extends JetConfig> flowProcess,
                              SourceCall<Void, Iterator<Entry>> sourceCall) {
    }

    @Override
    public void sinkPrepare(FlowProcess<? extends JetConfig> flowProcess,
                            SinkCall<Integer, Consumer<Entry>> sinkCall) {
    }
}
