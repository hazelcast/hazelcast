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

package com.hazelcast.client;

import com.hazelcast.spi.Callback;
import com.hazelcast.spi.InvocationBuilder;
import com.hazelcast.spi.Operation;

/**
 * @author mdogan 5/3/13
 */
public abstract class PartitionClientRequest extends ClientRequest {

    final void process() {
        final ClientEndpoint endpoint = getEndpoint();
        final Operation op = prepareOperation();
        op.setCallerUuid(endpoint.getUuid());
        final InvocationBuilder builder = clientEngine.createInvocationBuilder(getServiceName(), op, getPartition())
                .setReplicaIndex(getReplicaIndex()).setTryCount(100)
                .setCallback(new Callback<Object>() {
                    public void notify(Object object) {
                        endpoint.sendResponse(filter(object), getCallId());
                    }
                });
        builder.invoke();
    }

    protected abstract Operation prepareOperation();

    protected abstract int getPartition();

    protected abstract int getReplicaIndex();

    protected Object filter(Object response) {
        return response;
    }

}
