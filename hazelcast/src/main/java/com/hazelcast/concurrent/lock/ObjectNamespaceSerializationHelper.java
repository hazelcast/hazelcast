/*
 * Copyright (c) 2008-2017, Hazelcast, Inc. All Rights Reserved.
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

import com.hazelcast.internal.cluster.Versions;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.spi.DefaultObjectNamespace;
import com.hazelcast.spi.DistributedObjectNamespace;
import com.hazelcast.spi.ObjectNamespace;
import com.hazelcast.version.Version;

import java.io.IOException;

/**
 * Serialization helper to serialize/deserialize {@link DistributedObjectNamespace}
 * and {@link DefaultObjectNamespace} between 3.8 and 3.9 members compatibly.
 * <p>
 * This class is not needed for versions after 3.9.
 */
public final class ObjectNamespaceSerializationHelper {

    private ObjectNamespaceSerializationHelper() {
    }

    public static void writeNamespaceCompatibly(ObjectNamespace namespace, ObjectDataOutput out) throws IOException {
        Version version = out.getVersion();
        assert !version.isUnknown();

        if (version.isGreaterOrEqual(Versions.V3_9)) {
            if (namespace.getClass() == DefaultObjectNamespace.class) {
                out.writeObject(new DistributedObjectNamespace(namespace));
            } else {
                out.writeObject(namespace);
            }
        } else {
            if (namespace.getClass() == DistributedObjectNamespace.class) {
                out.writeObject(new DefaultObjectNamespace(namespace));
            } else {
                out.writeObject(namespace);
            }
        }
    }

    public static ObjectNamespace readNamespaceCompatibly(ObjectDataInput in) throws IOException {
        ObjectNamespace namespace = in.readObject();
        if (namespace.getClass() == DefaultObjectNamespace.class) {
            namespace = new DistributedObjectNamespace(namespace);
        }
        return namespace;
    }
}
