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

package com.hazelcast.internal.partition.impl;

import com.hazelcast.internal.partition.PartitionEventListener;
import com.hazelcast.partition.PartitionLostEvent;
import com.hazelcast.partition.PartitionLostListener;

class PartitionLostListenerAdapter implements PartitionEventListener<PartitionLostEvent> {

    private final PartitionLostListener listener;

    PartitionLostListenerAdapter(PartitionLostListener listener) {
        this.listener = listener;
    }

    @Override
    public void onEvent(PartitionLostEvent event) {
        listener.partitionLost(event);
    }

}
