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

package com.hazelcast.jet.stream.impl.pipeline;

import com.hazelcast.core.HazelcastInstance;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class StreamContext {

    private final HazelcastInstance instance;
    private final List<Runnable> streamListeners = new ArrayList<>();
    private final Set<Class> classes = new HashSet<>();

    public StreamContext(HazelcastInstance instance) {
        this.instance = instance;
    }

    public HazelcastInstance getHazelcastInstance() {
        return instance;
    }

    public List<Runnable> getStreamListeners() {
        return Collections.unmodifiableList(streamListeners);
    }

    public void addStreamListener(Runnable runnable) {
        streamListeners.add(runnable);
    }

    public void addClasses(Class... classes) {
        Collections.addAll(this.classes, classes);
    }

    public void addObjectClass(Object obj) {
        addClasses(obj.getClass());
    }

    public Set<Class> getClasses() {
        return Collections.unmodifiableSet(classes);
    }
}
