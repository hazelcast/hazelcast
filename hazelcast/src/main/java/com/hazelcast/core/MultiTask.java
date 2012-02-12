/*
 * Copyright (c) 2008-2012, Hazel Bilisim Ltd. All Rights Reserved.
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

package com.hazelcast.core;

import java.util.Collection;
import java.util.Set;
import java.util.concurrent.*;

public class MultiTask<V> extends DistributedTask {
    protected Collection<V> results = new CopyOnWriteArrayList<V>();

    public MultiTask(Callable<V> callable, Set<Member> members) {
        super(callable, members);
    }

    @Override
    public void onResult(Object result) {
        results.add((V) result);
    }

    @Override
    public Collection<V> get() throws ExecutionException, InterruptedException {
        super.get();
        return results;
    }

    @Override
    public Collection<V> get(long timeout, TimeUnit unit) throws InterruptedException,
            ExecutionException, TimeoutException {
        super.get(timeout, unit);
        return results;
    }
}
