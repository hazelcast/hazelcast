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

package com.hazelcast.spi;

import com.hazelcast.cache.impl.operation.PostJoinCacheOperation;
import com.hazelcast.client.impl.operations.PostJoinClientOperation;
import com.hazelcast.map.impl.operation.PostJoinMapOperation;
import com.hazelcast.spi.impl.eventservice.impl.operations.PostJoinRegistrationOperation;
import com.hazelcast.spi.impl.proxyservice.impl.operations.PostJoinProxyOperation;

import java.util.HashMap;
import java.util.Map;

/**
 * An interface to be implemented by post join {@link Operation}s, indicating the sequence in which
 * the post join operation must be applied.
 *
 * @see PostJoinAwareService#getPostJoinOperation()
 */
public interface PostJoinAwareOperation {

    /**
     * @return unique sequence number for applying this post join operation. Operations with a lower sequence
     * will be applied before ones with a higher sequence.
     */
    int getSequence();

    /**
     * Sequence of post join aware operations
     */
    @SuppressWarnings("checkstyle:magicnumber")
    final class Sequence {
        public static final Map<Class<? extends Operation>, Integer> POST_JOIN_OPS_SEQUENCE;

        static {
            HashMap<Class<? extends Operation>, Integer> postJoinOpsSequence = new HashMap<Class<? extends Operation>, Integer>();
            postJoinOpsSequence.put(PostJoinCacheOperation.class, 0);
            postJoinOpsSequence.put(PostJoinRegistrationOperation.class, 1);
            postJoinOpsSequence.put(PostJoinClientOperation.class, 2);
            postJoinOpsSequence.put(PostJoinProxyOperation.class, 3);
            postJoinOpsSequence.put(PostJoinMapOperation.class, 4);
            POST_JOIN_OPS_SEQUENCE = postJoinOpsSequence;
        }

        private Sequence() {

        }
    }
}
