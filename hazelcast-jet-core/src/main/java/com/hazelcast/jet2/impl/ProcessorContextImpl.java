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

package com.hazelcast.jet2.impl;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.jet2.ProcessorContext;

class ProcessorContextImpl implements ProcessorContext {

    private final HazelcastInstance instance;
    private final int parallelism;
    private final ClassLoader classLoader;

    public ProcessorContextImpl(HazelcastInstance instance, int parallelism, ClassLoader classLoader) {
        this.instance = instance;
        this.parallelism = parallelism;
        this.classLoader = classLoader;
    }

    @Override
    public HazelcastInstance getHazelcastInstance() {
        return instance;
    }

    @Override
    public int parallelism() {
        return parallelism;
    }

    @Override
    public ClassLoader getClassLoader() {
        return classLoader;
    }
}
