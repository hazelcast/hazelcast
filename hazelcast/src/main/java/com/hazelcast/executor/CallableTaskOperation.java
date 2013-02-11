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
import com.hazelcast.spi.impl.AbstractNamedOperation;

import java.io.IOException;
import java.util.concurrent.Callable;

/**
 * @mdogan 1/18/13
 */
public class CallableTaskOperation<V> extends AbstractNamedOperation {

    private Callable<V> callable;

    public CallableTaskOperation() {
    }

    public CallableTaskOperation(String name, Callable<V> callable) {
        super(name);
        this.callable = callable;
    }

    @Override
    public void beforeRun() throws Exception {
        if (callable instanceof HazelcastInstanceAware && getCallId() < 0) {
            ((HazelcastInstanceAware) callable).setHazelcastInstance(getNodeEngine().getHazelcastInstance());
        }
    }

    public void run() throws Exception {
        DistributedExecutorService service = getService();
        service.execute(name, callable, getResponseHandler());
    }

    @Override
    public void afterRun() throws Exception {
    }

    @Override
    public boolean returnsResponse() {
        return false;
    }

    @Override
    protected void writeInternal(ObjectDataOutput out) throws IOException {
        super.writeInternal(out);
        out.writeObject(callable);
    }

    @Override
    protected void readInternal(ObjectDataInput in) throws IOException {
        super.readInternal(in);
        callable = in.readObject();
    }
}
