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

package com.hazelcast.jet.internal.impl.container.task;

import com.hazelcast.jet.internal.api.executor.Task;

public abstract class AbstractTask implements Task {
    protected volatile ClassLoader contextClassLoader;

    @Override
    public void setThreadContextClassLoaders(ClassLoader classLoader) {
        this.contextClassLoader = classLoader;
    }

    @Override
    public void init() {

    }

    @Override
    public void destroy() {

    }

    @Override
    public void interrupt(Throwable error) {

    }

    @Override
    public void beforeProcessing() {

    }

    public void finalizeTask() {

    }
}
