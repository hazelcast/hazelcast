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

package com.hazelcast.client.proxy;

import com.hazelcast.client.spi.ClientContext;
import com.hazelcast.client.spi.ClientInvocationService;
import com.hazelcast.client.spi.ClientProxy;
import com.hazelcast.mapreduce.Job;
import com.hazelcast.mapreduce.JobTracker;
import com.hazelcast.mapreduce.KeyValueSource;
import com.hazelcast.mapreduce.impl.AbstractJob;
import com.hazelcast.mapreduce.impl.client.ClientMapReduceRequest;
import com.hazelcast.mapreduce.process.ProcessJob;

import java.util.ArrayList;

public class ClientMapReduceProxy extends ClientProxy implements JobTracker {

    public ClientMapReduceProxy(String serviceName, String objectName) {
        super(serviceName, objectName);
    }

    @Override
    protected void onDestroy() {

    }

    @Override
    public <K, V> Job<K, V> newJob(KeyValueSource<K, V> source) {
        return new ClientJob<K, V>(getName(), source);
    }

    @Override
    public <K, V> ProcessJob<K, V> newProcessJob(KeyValueSource<K, V> source) {
        // TODO
        return null;
    }

    private class ClientJob<KeyIn, ValueIn> extends AbstractJob<KeyIn, ValueIn> {

        public ClientJob(String name, KeyValueSource<KeyIn, ValueIn> keyValueSource) {
            super(name, keyValueSource);
        }

        @Override
        protected void invokeTask() throws Exception {
            ClientContext context = getContext();
            ClientInvocationService cis = context.getInvocationService();
            ClientMapReduceRequest request = new ClientMapReduceRequest(name, new ArrayList(keys), predicate,
                    mapper, combinerFactory, reducerFactory, keyValueSource, chunkSize);
            cis.invokeOnRandomTarget(request);
        }

    }

}
