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

package com.hazelcast.wan;

import com.hazelcast.config.WanQueueFullBehavior;
import com.hazelcast.core.HazelcastException;

/**
 * A {@link com.hazelcast.core.HazelcastException} that
 * is thrown when the wan replication queues are full
 *
 * This exception is only thrown when WAN is configured with
 * {@link WanQueueFullBehavior#THROW_EXCEPTION}
 */
public class WanQueueFullException extends HazelcastException {

    public WanQueueFullException(String message) {
        super(message);
    }
}
