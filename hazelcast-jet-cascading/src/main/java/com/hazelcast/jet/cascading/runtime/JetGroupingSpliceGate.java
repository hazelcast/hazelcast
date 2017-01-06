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
import cascading.flow.stream.element.GroupingSpliceGate;
import cascading.pipe.Splice;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.tuple.util.TupleBuilder;

import static cascading.tuple.util.TupleViews.createNarrow;
import static cascading.tuple.util.TupleViews.reset;

public abstract class JetGroupingSpliceGate extends GroupingSpliceGate {
    protected JetGroupingSpliceGate(FlowProcess flowProcess, Splice splice) {
        super(flowProcess, splice);
    }

    @Override
    protected TupleBuilder createDefaultNarrowBuilder(final Fields incomingFields, final Fields narrowFields) {
        final Tuple result;
        //incomingFields.getPos() is not thread-safe
        synchronized (incomingFields) {
            result = createNarrow(incomingFields.getPos(narrowFields));
        }

        return new TupleBuilder() {
            @Override
            public Tuple makeResult(Tuple input, Tuple output) {
                return reset(result, input);
            }
        };
    }
}
