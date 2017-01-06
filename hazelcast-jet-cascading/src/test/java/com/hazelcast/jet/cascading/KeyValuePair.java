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
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntry;
import cascading.tuple.io.ValueTuple;
import com.hazelcast.jet.JetConfig;
import com.hazelcast.jet.Outbox;

import java.io.IOException;
import java.util.AbstractMap;
import java.util.Iterator;
import java.util.Map;

import static com.hazelcast.util.ExceptionUtil.rethrow;


public class KeyValuePair extends Scheme<JetConfig, Iterator<Map.Entry>,
        Outbox, Void, Integer> {

    public static final Fields DEFAULT_FIELDS = new Fields("key", "value");

    public KeyValuePair() {
        super(DEFAULT_FIELDS, DEFAULT_FIELDS);
    }

    public KeyValuePair(Fields sourceFields) {
        super(sourceFields);
    }

    public KeyValuePair(Fields sourceFields, Fields sinkFields) {
        super(sourceFields, sinkFields);

        if (sourceFields.size() > 2) {
            throw new IllegalArgumentException("Source fields must not be greater than 2");
        }
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
    public void setSinkFields(Fields sinkFields) {
        if (sinkFields.size() > 2) {
            throw new IllegalArgumentException("There can't be more than 2 sink fields");
        }
        super.setSinkFields(sinkFields);
    }

    @Override
    public boolean source(FlowProcess<? extends JetConfig> flowProcess, SourceCall<Void,
            Iterator<Map.Entry>> sourceCall) throws IOException {
        Iterator<Map.Entry> iterator = sourceCall.getInput();
        if (!iterator.hasNext()) {
            return false;
        }
        Map.Entry pair = iterator.next();
        if (getSourceFields().size() == 1) {
            sourceCall.getIncomingEntry().setObject(0, pair.getValue());
        } else {
            sourceCall.getIncomingEntry().setObject(0, pair.getKey());
            sourceCall.getIncomingEntry().setObject(1, pair.getValue());
        }
        return true;
    }

    @Override
    public void sink(FlowProcess<? extends
            JetConfig> flowProcess, SinkCall<Integer, Outbox> sinkCall) throws IOException {
        Outbox outbox = sinkCall.getOutput();
        TupleEntry outgoing = sinkCall.getOutgoingEntry();
        try {
            Tuple tuple = outgoing.getTuple();
            if (getSinkFields().size() == 2) {
                outbox.add(new AbstractMap.SimpleImmutableEntry<>(tuple.getObject(0), tuple.getObject(1)));
            } else if (getSinkFields().size() == 1) {
                outbox.add(new AbstractMap.SimpleImmutableEntry<>(tuple.getObject(0), ValueTuple.NULL));
            }
        } catch (Exception e) {
            throw rethrow(e);
        }
    }

}
