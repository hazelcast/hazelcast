/*
 * Copyright (c) 2008-2019, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.spi.impl.proxyservice.impl.operations;

import com.hazelcast.core.Offloadable;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;
import com.hazelcast.spi.CallStatus;
import com.hazelcast.spi.Offload;
import com.hazelcast.spi.Operation;
import com.hazelcast.spi.RemoteService;
import com.hazelcast.spi.impl.SpiDataSerializerHook;
import com.hazelcast.spi.impl.proxyservice.impl.ProxyServiceImpl;
import com.hazelcast.util.ExceptionUtil;

import java.io.IOException;

import static com.hazelcast.spi.CallStatus.DONE_RESPONSE;

public class DistributedObjectDestroyOperation
        extends Operation implements IdentifiedDataSerializable {

    private String serviceName;
    private String name;

    public DistributedObjectDestroyOperation() {
    }

    public DistributedObjectDestroyOperation(String serviceName, String name) {
        this.serviceName = serviceName;
        this.name = name;
    }

    @Override
    public CallStatus call() throws Exception {
        Offloadable offloadable = getOrNullOffloadableService();
        if (offloadable != null) {
            return new OffloadedDestroyer(offloadable);
        } else {
            destroyLocalDistributedObject();
            return DONE_RESPONSE;
        }
    }

    /**
     * @return {@link Offloadable} service object if service is implemented
     * {@link Offloadable} interface, otherwise return null to
     * indicate service is not an instance of {@link Offloadable}
     */
    private Offloadable getOrNullOffloadableService() {
        RemoteService service = getNodeEngine().getService(serviceName);
        if (service instanceof Offloadable) {
            return ((Offloadable) service);
        } else {
            return null;
        }
    }

    private void destroyLocalDistributedObject() {
        ProxyServiceImpl proxyService = getService();
        proxyService.destroyLocalDistributedObject(serviceName, name, false);
    }

    /**
     * Offloads destroy call to not to block {@code GenericOperationThread}.
     */
    private final class OffloadedDestroyer extends Offload {

        private final String executorName;

        public OffloadedDestroyer(Offloadable offloadable) {
            super(DistributedObjectDestroyOperation.this);
            this.executorName = offloadable.getExecutorName();
        }

        @Override
        public void start() {
            try {
                executionService.execute(executorName, new Runnable() {
                    @Override
                    public void run() {
                        try {
                            destroyLocalDistributedObject();
                            sendResponse(true);
                        } catch (Throwable throwable) {
                            sendResponse(throwable);
                        }
                    }
                });
            } catch (Throwable throwable) {
                throw ExceptionUtil.rethrow(throwable);
            }
        }
    }

    @Override
    public Object getResponse() {
        return Boolean.TRUE;
    }

    @Override
    protected void writeInternal(ObjectDataOutput out) throws IOException {
        super.writeInternal(out);
        out.writeUTF(serviceName);
        out.writeObject(name);
        // writing as object for backward-compatibility
    }

    @Override
    protected void readInternal(ObjectDataInput in) throws IOException {
        super.readInternal(in);
        serviceName = in.readUTF();
        name = in.readObject();
    }

    @Override
    public int getFactoryId() {
        return SpiDataSerializerHook.F_ID;
    }

    @Override
    public int getId() {
        return SpiDataSerializerHook.DIST_OBJECT_DESTROY;
    }
}
