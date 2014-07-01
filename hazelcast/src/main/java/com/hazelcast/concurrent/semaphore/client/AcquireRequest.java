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

package com.hazelcast.concurrent.semaphore.client;

import com.hazelcast.concurrent.semaphore.operations.AcquireOperation;
import com.hazelcast.nio.serialization.PortableReader;
import com.hazelcast.nio.serialization.PortableWriter;
import com.hazelcast.security.permission.ActionConstants;
import com.hazelcast.security.permission.SemaphorePermission;
import com.hazelcast.spi.Operation;

import java.io.IOException;
import java.security.Permission;
import java.util.concurrent.TimeUnit;

public class AcquireRequest extends SemaphoreRequest {

    private long timeout = -1;

    public AcquireRequest() {
    }

    public AcquireRequest(String name, int permitCount, long timeout) {
        super(name, permitCount);
        this.timeout = timeout;
    }

    @Override
    protected Operation prepareOperation() {
        return new AcquireOperation(name, permitCount, timeout);
    }

    @Override
    public int getClassId() {
        return SemaphorePortableHook.ACQUIRE;
    }

    @Override
    public void write(PortableWriter writer) throws IOException {
        super.write(writer);
        writer.writeLong("t", timeout);
    }

    @Override
    public void read(PortableReader reader) throws IOException {
        super.read(reader);
        timeout = reader.readLong("t");
    }

    @Override
    public Permission getRequiredPermission() {
        return new SemaphorePermission(name, ActionConstants.ACTION_ACQUIRE);
    }

    @Override
    public String getMethodName() {
        if (timeout != -1) {
            return "tryAcquire";
        }
        return "acquire";
    }

    @Override
    public Object[] getParameters() {
        if (permitCount == 1 ) {
            if (timeout > 0) {
                return new Object[]{timeout, TimeUnit.MILLISECONDS}; // tryAcquire(timeout, TimeUnit)
            }
            return null; // acquire(), tryAcquire()
        } else if (timeout > 0) {
            return new Object[]{permitCount, timeout, TimeUnit.MILLISECONDS}; //tryAcquire(permit, timeout, TimeUnit)
        }
        return super.getParameters(); // acquire(permit), tryAcquire(permit)
    }
}
