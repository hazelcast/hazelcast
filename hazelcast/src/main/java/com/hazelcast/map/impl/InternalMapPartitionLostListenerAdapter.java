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

import com.hazelcast.map.MapPartitionLostEvent;
import com.hazelcast.map.listener.MapPartitionLostListener;
import com.hazelcast.spi.annotation.PrivateApi;

/**
 * Responsible for dispatching the event to the public api
 *
 * @see com.hazelcast.map.listener.MapPartitionLostListener
 */
@PrivateApi
class InternalMapPartitionLostListenerAdapter
        implements ListenerAdapter {

    private final MapPartitionLostListener partitionLostListener;

    InternalMapPartitionLostListenerAdapter(MapPartitionLostListener partitionLostListener) {
        this.partitionLostListener = partitionLostListener;
    }

    @Override
    public void onEvent(Object event) {
        partitionLostListener.partitionLost((MapPartitionLostEvent) event);
    }

}
