/*
 * Copyright (c) 2008-2023, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.client.impl.spi.impl;

import com.hazelcast.core.HazelcastException;

/**
 * Signals that the invocation might contain Compact classes, and it would
 * not be safe to send that invocation now to make sure that the invariant
 * regarding not sending the data before the schemas are hold while the client
 * reconnects or retries urgent invocations.
 */
class InvocationMightContainCompactDataException extends HazelcastException {
    InvocationMightContainCompactDataException(ClientInvocation invocation) {
        super("The invocation" + invocation + " might contain Compact serialized "
                + "data and it is not safe to invoke it when the client is not "
                + "yet initialized on the cluster");
    }
}
