/*
 * Copyright (c) 2008-2013, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.executor;

import com.hazelcast.core.HazelcastInstanceAware;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.spi.Operation;

import java.io.IOException;
import java.util.concurrent.Callable;

/**
 * @mdogan 1/18/13
 */
public abstract class BaseCallableTaskOperation<V> extends Operation {

    protected String name;
    protected Callable<V> callable;

    public BaseCallableTaskOperation() {
    }

    public BaseCallableTaskOperation(String name, Callable<V> callable) {
        this.name = name;
        this.callable = callable;
    }

    @Override
    public final void beforeRun() throws Exception {
        if (callable instanceof HazelcastInstanceAware) {
            ((HazelcastInstanceAware) callable).setHazelcastInstance(getNodeEngine().getHazelcastInstance());
        }
    }

    public final void run() throws Exception {
        DistributedExecutorService service = getService();
        service.execute(name, callable, getResponseHandler());
    }

    @Override
    public final void afterRun() throws Exception {
    }

    @Override
    public final boolean returnsResponse() {
        return false;
    }

    @Override
    public final Object getResponse() {
        return null;
    }

    @Override
    protected void writeInternal(ObjectDataOutput out) throws IOException {
        out.writeUTF(name);
        out.writeObject(callable);
    }

    @Override
    protected void readInternal(ObjectDataInput in) throws IOException {
        name = in.readUTF();
        callable = in.readObject();
    }
}
