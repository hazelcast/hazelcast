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

package com.hazelcast.jet.stream.impl.pipeline;

import com.hazelcast.jet.JetInstance;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class StreamContext {

    private final JetInstance instance;
    private final List<Runnable> streamListeners = new ArrayList<>();

    public StreamContext(JetInstance instance) {
        this.instance = instance;
    }

    public JetInstance getJetInstance() {
        return instance;
    }

    public List<Runnable> getStreamListeners() {
        return Collections.unmodifiableList(streamListeners);
    }

    public void addStreamListener(Runnable runnable) {
        streamListeners.add(runnable);
    }

}
