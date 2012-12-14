/*
 * Copyright (c) 2008-2012, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.impl;

import com.hazelcast.impl.executor.ParallelExecutor;

public class NamedExecutorService {

    private final String name;
    private final ParallelExecutor parallelExecutor;
    private final ExecutionLoadBalancer executionLoadBalancer;

    public NamedExecutorService(String name, ParallelExecutor parallelExecutor) {
        this.name = name;
        this.parallelExecutor = parallelExecutor;
        this.executionLoadBalancer = new RoundRobinLoadBalancer();
    }

    public void appendState(StringBuffer sbState) {
        sbState.append("\nExecutor." + name + ".size=" + parallelExecutor.getPoolSize());
    }

    public void stop() {
        parallelExecutor.shutdown();
    }

    public void execute(Runnable runnable) {
        parallelExecutor.execute(runnable);
    }

    public void executeOrderedRunnable(int hash, Runnable runnable) {
        parallelExecutor.execute(runnable, hash);
    }

    public ExecutionLoadBalancer getExecutionLoadBalancer() {
        return executionLoadBalancer;
    }
}
