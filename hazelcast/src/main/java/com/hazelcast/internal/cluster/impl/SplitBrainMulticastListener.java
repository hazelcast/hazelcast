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

package com.hazelcast.internal.cluster.impl;

import com.hazelcast.instance.Node;

import java.util.concurrent.BlockingDeque;

/**
 * Listens for {@code SplitBrainJoinMessage}s and adds them for processing by split brain handler. Each messages is added
 * to the head of a {@code BlockingDeque}, so that polling the {@code BlockingDeque} will return the message received last
 * (thus operating as a stack).
 */
public class SplitBrainMulticastListener implements MulticastListener {

    private final Node node;
    private final BlockingDeque<SplitBrainJoinMessage> deque;

    public SplitBrainMulticastListener(Node node, BlockingDeque<SplitBrainJoinMessage> deque) {
        this.node = node;
        this.deque = deque;
    }

    public void onMessage(Object msg) {
        if (msg instanceof SplitBrainJoinMessage) {
            SplitBrainJoinMessage joinRequest = (SplitBrainJoinMessage) msg;
            if (!node.getThisAddress().equals(joinRequest.getAddress())) {
                deque.addFirst(joinRequest);
            }
        }
    }
}
