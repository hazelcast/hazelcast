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

package com.hazelcast.jet.cascading;

import cascading.flow.FlowProcess;
import cascading.scheme.Scheme;
import cascading.scheme.SinkCall;
import cascading.scheme.SourceCall;
import cascading.tap.Tap;
import cascading.tuple.Fields;
import cascading.tuple.TupleEntry;
import com.hazelcast.jet.JetConfig;
import com.hazelcast.jet.Outbox;

import java.io.IOException;
import java.util.AbstractMap;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import static com.hazelcast.util.ExceptionUtil.rethrow;

public class TextLine extends Scheme<JetConfig, Iterator<Map.Entry>,
        Outbox, Void, Integer> {

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

        //TODO: verify fields
    }

    @Override
    public void sourceConfInit(FlowProcess<? extends JetConfig> flowProcess,
                               Tap<JetConfig, Iterator<Map.Entry>, Outbox> tap,
                               JetConfig conf) {

    }

    @Override
    public void sinkConfInit(FlowProcess<? extends JetConfig> flowProcess,
                             Tap<JetConfig, Iterator<Map.Entry>, Outbox> tap,
                             JetConfig conf) {

    }

    @Override
    public void sourcePrepare(FlowProcess<? extends JetConfig> flowProcess,
                              SourceCall<Void, Iterator<Map.Entry>> sourceCall)
            throws IOException {
    }

    @Override
    public boolean source(FlowProcess<? extends JetConfig> flowProcess, SourceCall<Void,
            Iterator<Map.Entry>> sourceCall) throws IOException {
        Iterator<Map.Entry> iterator = sourceCall.getInput();
        if (!iterator.hasNext()) {
            return false;
        }
        Map.Entry entry = iterator.next();
        if (getSourceFields().size() == 1) {
            sourceCall.getIncomingEntry().setObject(0, entry.getValue());
        } else {
            sourceCall.getIncomingEntry().setObject(0, entry.getKey());
            sourceCall.getIncomingEntry().setObject(1, entry.getValue());
        }
        return true;
    }

    @Override
    public void sinkPrepare(FlowProcess<? extends JetConfig> flowProcess,
                            SinkCall<Integer, Outbox> sinkCall) throws IOException {
    }

    @Override
    public void sink(FlowProcess<? extends
            JetConfig> flowProcess, SinkCall<Integer, Outbox> sinkCall) throws IOException {
        Outbox outbox = sinkCall.getOutput();
        TupleEntry outgoing = sinkCall.getOutgoingEntry();
        try {
            outbox.add(new AbstractMap.SimpleImmutableEntry<>(COUNTER.getAndIncrement(), outgoing.getTuple().toString()));
        } catch (Exception e) {
            throw rethrow(e);
        }
    }

}
