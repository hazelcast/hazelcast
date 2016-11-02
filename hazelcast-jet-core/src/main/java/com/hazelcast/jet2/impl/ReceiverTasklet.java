/*
 * Copyright (c) 2008-2016, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.jet2.impl;

import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;

import static com.hazelcast.jet2.impl.DoneItem.DONE_ITEM;

public class ReceiverTasklet implements Tasklet {

    private static int counter = 0;
    private final Queue<Payload> inbox = new ConcurrentLinkedQueue<>();
    //TODO: MPSCQueue does not implement peek() yet
    private final OutboundCollector collector;
    private int senderCount;
    private final ProgressTracker tracker = new ProgressTracker();

    public ReceiverTasklet(OutboundCollector collector, int senderCount) {
        this.collector = collector;
        this.senderCount = senderCount;
    }

    @Override
    public void init() {

    }

    void offer(Payload item) {
        inbox.offer(item);
    }

    @Override
    public ProgressState call() throws Exception {
        tracker.reset();
        tracker.notDone();
        for (Payload payload; (payload = inbox.peek()) != null; ) {
            Object item = payload.getItem();
            if (item != DONE_ITEM) {
                collector.offer(item, payload.getPartitionId());
            } else {
                if (--senderCount == 0) {
                    collector.close();
                    return ProgressState.DONE;
                }
            }
            inbox.remove();
            tracker.madeProgress(true);
        }
        return tracker.toProgressState();
    }
}
