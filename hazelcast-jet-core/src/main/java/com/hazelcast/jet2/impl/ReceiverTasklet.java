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

import com.hazelcast.nio.serialization.Data;
import com.hazelcast.spi.serialization.SerializationService;

import java.util.AbstractMap.SimpleImmutableEntry;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;

import static com.hazelcast.jet2.impl.DoneItem.DONE_ITEM;

public class ReceiverTasklet implements Tasklet {

    private final Queue<Map.Entry<Data, Integer>> inbox = new ConcurrentLinkedQueue<>();
    private final SerializationService serializationService;
    //TODO: MPSCQueue does not implement peek() yet
    private final OutboundCollector collector;
    private int remainingSenders;
    private final ProgressTracker tracker = new ProgressTracker();

    private Object currItem;

    public ReceiverTasklet(SerializationService serializationService,
                           OutboundCollector collector, int remainingSenders) {
        this.serializationService = serializationService;
        this.collector = collector;
        this.remainingSenders = remainingSenders;
    }

    @Override
    public void init() {
    }

    void offer(Data item, int partitionCount) {
        inbox.offer(new SimpleImmutableEntry<>(item, partitionCount));
    }

    @Override
    public ProgressState call() {
        tracker.reset();
        tracker.notDone();
        for (Map.Entry<Data, Integer> entry; (entry = inbox.peek()) != null; ) {
            if (currItem == null) {
                currItem = serializationService.toObject(entry.getKey());
            }
            if (currItem == DONE_ITEM) {
                remainingSenders--;
                tracker.madeProgress();
            } else {
                ProgressState offered = collector.offer(currItem, entry.getValue());
                tracker.mergeWith(offered);
                if (!offered.isDone()) {
                    break;
                }
            }
            currItem = null;
            inbox.remove();
        }

        if (remainingSenders == 0) {
            ProgressState closed = collector.close();
            tracker.mergeWith(collector.close());
            if (closed.isDone()) {
                return ProgressState.DONE;
            }
        }
        return tracker.toProgressState();
    }

}
