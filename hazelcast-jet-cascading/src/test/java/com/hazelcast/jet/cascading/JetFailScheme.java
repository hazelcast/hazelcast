/*
 * Copyright (c) 2007-2016 Concurrent, Inc. All Rights Reserved.
 *
 * Project and contact information: http://www.cascading.org/
 *
 * This file is part of the Cascading project.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.hazelcast.jet.cascading;

import cascading.flow.FlowProcess;
import cascading.scheme.SinkCall;
import cascading.scheme.SourceCall;
import cascading.tap.TapException;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import com.hazelcast.jet.config.JobConfig;
import com.hazelcast.jet.runtime.OutputCollector;
import com.hazelcast.jet.io.Pair;

import java.io.IOException;
import java.util.Iterator;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 *
 */
public class JetFailScheme extends TextLine {
    static AtomicBoolean sourceFired = new AtomicBoolean(false);
    static AtomicBoolean sinkFired = new AtomicBoolean(false);

    public JetFailScheme(Fields sourceFields) {
        super(sourceFields);
        sourceFired.set(false);
        sinkFired.set(false);
    }

    @Override
    public boolean source(FlowProcess<? extends JobConfig> flowProcess,
                          SourceCall<Void, Iterator<Pair>> sourceCall) throws IOException {
        if (sourceFired.compareAndSet(false, true)) {
            throw new TapException("fail", new Tuple("bad data"));
        }

        return super.source(flowProcess, sourceCall);
    }

    @Override
    public void sink(FlowProcess<? extends JobConfig> flowProcess, SinkCall<Integer,
            OutputCollector<Pair>> sinkCall) throws IOException {
        if (sinkFired.compareAndSet(false, true)) {
            throw new TapException("fail", new Tuple("bad data"));
        }

        super.sink(flowProcess, sinkCall);
    }
}
