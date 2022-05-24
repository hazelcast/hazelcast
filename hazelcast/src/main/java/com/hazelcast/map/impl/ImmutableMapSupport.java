/*
 * Copyright (c) 2008-2022, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.map.impl;
import com.hazelcast.internal.serialization.Data;
import com.hazelcast.map.Immutable;
import com.hazelcast.spi.impl.operationservice.Operation;
public final class ImmutableMapSupport {

    private static final Class<?> CONSTABLE;
    static {
        Class<?> constable = null;
        try {
            constable = ClassLoader.getSystemClassLoader().loadClass("java.lang.constant.Constable");
        } catch (ClassNotFoundException e) {
            // Ignore, old version of java
        }
        CONSTABLE = constable;
    }

    private ImmutableMapSupport() { }

    public static boolean isConsideredImmutable(Object dataValue, Operation operation) {
        // Before any data is returned to a client, it is the responsibility of the immediately called method to
        // convert it into the Object type which that client expects using toObject.
        // Therefor we can consider Data as immutable because it will still be converted at this point.
        // THis allows us on a remote call to not unnecessarily convert to the Object format and then to Data format
        // across the wire
        if (dataValue instanceof Data) {
            return true;
        }
        if (dataValue instanceof Immutable) {
            return true;
        }
        // In Java 12, this interface was added to String, Number...
        if (CONSTABLE != null) {
            if (CONSTABLE.isInstance(dataValue)) {
                return true;
            }
        } else if (dataValue instanceof String || dataValue instanceof Number) {
            return true;
        }
        // Any code that executes on a different node than the caller has no capability to mutate that object.
        // So, we can consider all remote objects to be immutable. This removes unnecessary defensive copies
        return !operation.getNodeEngine().getLocalMember().getUuid().equals(operation.getCallerUuid())
            || !operation.executedLocally();
    }
    public static Object defensiveCopy(Object dataValue, MapServiceContext context) {
        return context.toObject(context.toData(dataValue));
    }
}
