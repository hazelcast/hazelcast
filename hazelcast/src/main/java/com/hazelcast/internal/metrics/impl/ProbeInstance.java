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

package com.hazelcast.internal.metrics.impl;

import com.hazelcast.internal.metrics.ProbeFunction;

import java.lang.ref.WeakReference;

/**
 * A Probe Instance is an actual instance of a probe.
 *
 * A probe instance contains:
 * <ol>
 * <li>A source object, e.g. an OperationService instance</li>
 * <li>A ProbeFunction: e.g. an {@link com.hazelcast.internal.metrics.LongProbeFunction} that retrieves the number of
 * executed operations.</li>
 * </ol>
 *
 * @param <S>
 */
class ProbeInstance<S> {

    final String name;
    private volatile ProbeFunction function;
    private volatile Object source;

    ProbeInstance(String name, Object source, ProbeFunction function) {
        this.name = name;
        this.function = function;
        this.source = source;
    }

    public void setSource(Object source) {
        this.source = source;
    }

    public ProbeFunction getFunction() {
        return function;
    }

    public S getSource() {
        Object source = this.source;
        return (S) (source instanceof WeakReference ? ((WeakReference) source).get() : source);
    }

    public void destroy() {
        function = null;
        source = null;
    }

    public void overwrite(Object source, ProbeFunction function) {
        this.source = source;
        this.function = function;
    }
}
