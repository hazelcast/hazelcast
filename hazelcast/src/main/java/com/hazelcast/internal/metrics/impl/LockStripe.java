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

package com.hazelcast.internal.metrics.impl;

import static com.hazelcast.internal.util.HashUtil.hashToIndex;
import static java.lang.System.identityHashCode;

/**
 * A stripe of locks to synchronize on a source object.
 *
 * We don't want to lock on 'source' objects because we don't own them, to prevent running into unnecessary lock contention
 * issues.
 *
 * We rely on the identity hashcode of the object and not on the {@link Object#hashCode()} method to prevent running into
 * faulty or thread-unsafe implementations.
 */
class LockStripe {

    private static final int STRIPE_LENGTH = 20;

    private final Object[] stripe = new Object[STRIPE_LENGTH];

    LockStripe() {
        for (int k = 0; k < stripe.length; k++) {
            stripe[k] = new Object();
        }
    }

    Object getLock(Object source) {
        int hash = identityHashCode(source);
        return stripe[hashToIndex(hash, stripe.length)];
    }
}
