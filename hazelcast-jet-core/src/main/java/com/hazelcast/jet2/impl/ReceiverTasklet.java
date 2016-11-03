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
import java.util.concurrent.atomic.AtomicInteger;

import static com.hazelcast.jet2.impl.DoneItem.DONE_ITEM;

public class ReceiverTasklet implements Tasklet {

    private final Queue<Payload> inbox = new ConcurrentLinkedQueue<>();
    //TODO: MPSCQueue does not implement peek() yet
    private final OutboundCollector collector;
    private int remainingSenders;
    private final ProgressTracker tracker = new ProgressTracker();

    public ReceiverTasklet(OutboundCollector collector, int remainingSenders) {
        this.collector = collector;
        this.remainingSenders = remainingSenders;
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
            if (item == DONE_ITEM) {
                remainingSenders--;
                tracker.madeProgress();
            } else {
                ProgressState offered = collector.offer(item, payload.getPartitionId());
                tracker.update(offered);
                if (!offered.isDone()) {
                    break;
                }
            }
            inbox.remove();
        }

        if (remainingSenders == 0) {
            ProgressState closed = collector.close();
            tracker.update(collector.close());
            if (closed.isDone()) {
                return ProgressState.DONE;
            }
        }
        return tracker.toProgressState();
    }

}
