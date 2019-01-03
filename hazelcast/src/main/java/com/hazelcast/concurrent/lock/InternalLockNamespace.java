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

package com.hazelcast.concurrent.lock;

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.SerializableByConvention;
import com.hazelcast.spi.ObjectNamespace;
import com.hazelcast.spi.impl.operationparker.impl.OperationParkerImpl;

import java.io.IOException;

import static com.hazelcast.nio.serialization.SerializableByConvention.Reason.PUBLIC_API;

/**
 * A specialization of {@link ObjectNamespace} intended to be used by ILock proxies.
 *
 * It intentionally <b>does not</b> use {@link #name} field in <code>equals()</code>
 * and <code>hashcode()</code> methods. This is a hack to share a single instance of
 * {@link LockStore} for all ILocks proxies (per partition).
 *
 * Discussion:
 *
 * If we included the name in equals()/hashcode() methods then each ILock would create
 * its own {@link LockStoreImpl}. Each <code>LockStoreImpl</code> contains additional
 * mapping key -&gt; LockResource. However ILock proxies always use a single key only
 * thus creating a <code>LockStoreImpl</code> for each ILock would introduce an unnecessary
 * indirection and memory waster. Also each LockStoreImpl is maintaining its own
 * scheduler for lock eviction purposes, etc.
 *
 * I originally wanted to remove the <code>name</code> field and use a constant,
 * but it has side-effects - for example when a ILock proxy is destroyed then you
 * want to destroy all pending {@link com.hazelcast.spi.BlockingOperation}
 *
 * @see LockStoreContainer#getOrCreateLockStore(ObjectNamespace)
 * @see OperationParkerImpl#cancelParkedOperations(String, Object, Throwable)
 *
 */
@SerializableByConvention(PUBLIC_API)
public final class InternalLockNamespace implements ObjectNamespace {

    private String name;

    public InternalLockNamespace() {
    }

    public InternalLockNamespace(String name) {
        this.name = name;
    }

    @Override
    public String getServiceName() {
        return LockService.SERVICE_NAME;
    }

    @Override
    public String getObjectName() {
        return name;
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        out.writeUTF(name);
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        name = in.readUTF();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        //warning: this intentionally does not use name field see class-level JavaDoc above
        return true;
    }

    @Override
    public int hashCode() {
        //warning: this intentionally does not use name field see class-level JavaDoc above
        return getServiceName().hashCode();
    }

    @Override
    public String toString() {
        return "InternalLockNamespace{service='" + LockService.SERVICE_NAME + '\'' + ", objectName=" + name + '}';
    }
}
