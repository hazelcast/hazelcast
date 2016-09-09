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
import com.hazelcast.jet.config.JobConfig;
import com.hazelcast.jet.impl.util.JetUtil;
import com.hazelcast.jet.io.Pair;
import com.hazelcast.jet.runtime.JetPair;
import com.hazelcast.jet.runtime.OutputCollector;

import java.io.IOException;
import java.util.Iterator;
import java.util.concurrent.atomic.AtomicInteger;

import static com.hazelcast.jet.impl.util.JetUtil.unchecked;

public class TextLine extends Scheme<JobConfig, Iterator<Pair>,
        OutputCollector<Pair>, Void, Integer> {

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
    public void sourceConfInit(FlowProcess<? extends JobConfig> flowProcess,
                               Tap<JobConfig, Iterator<Pair>, OutputCollector<Pair>> tap,
                               JobConfig conf) {

    }

    @Override
    public void sinkConfInit(FlowProcess<? extends JobConfig> flowProcess,
                             Tap<JobConfig, Iterator<Pair>, OutputCollector<Pair>> tap,
                             JobConfig conf) {

    }

    @Override
    public void sourcePrepare(FlowProcess<? extends JobConfig> flowProcess,
                              SourceCall<Void, Iterator<Pair>> sourceCall)
            throws IOException {
    }

    @Override
    public boolean source(FlowProcess<? extends JobConfig> flowProcess, SourceCall<Void,
            Iterator<Pair>> sourceCall) throws IOException {
        Iterator<Pair> iterator = sourceCall.getInput();
        if (!iterator.hasNext()) {
            return false;
        }
        Pair pair = iterator.next();
        if (getSourceFields().size() == 1) {
            sourceCall.getIncomingEntry().setObject(0, pair.getValue());
        } else {
            sourceCall.getIncomingEntry().setObject(0, pair.getKey());
            sourceCall.getIncomingEntry().setObject(1, pair.getValue());
        }
        return true;
    }

    @Override
    public void sinkPrepare(FlowProcess<? extends JobConfig> flowProcess,
                            SinkCall<Integer, OutputCollector<Pair>> sinkCall) throws IOException {
    }

    @Override
    public void sink(FlowProcess<? extends
            JobConfig> flowProcess, SinkCall<Integer, OutputCollector<Pair>> sinkCall) throws IOException {
        OutputCollector<Pair> outputCollector = sinkCall.getOutput();
        TupleEntry outgoing = sinkCall.getOutgoingEntry();
        try {
            outputCollector.collect(new JetPair(COUNTER.getAndIncrement(), outgoing.getTuple().toString()));
        } catch (Exception e) {
            throw unchecked(e);
        }
    }

}
