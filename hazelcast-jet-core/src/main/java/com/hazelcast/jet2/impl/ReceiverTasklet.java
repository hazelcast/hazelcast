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

import com.hazelcast.internal.serialization.InternalSerializationService;
import com.hazelcast.internal.serialization.impl.ObjectDataInputStream;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.ArrayDeque;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;

import static com.hazelcast.jet2.impl.DoneItem.DONE_ITEM;
import static com.hazelcast.util.ExceptionUtil.rethrow;

public class ReceiverTasklet implements Tasklet {

    //TODO: MPSCQueue does not implement peek() yet
    private final Queue<ByteArrayInputStream> incoming = new ConcurrentLinkedQueue<>();
    private final InternalSerializationService serializationService;
    private final OutboundCollector collector;
    private final ProgressTracker tracker = new ProgressTracker();
    private final ArrayDeque<Object> inbox = new ArrayDeque<>();

    private int remainingSenders;

    public ReceiverTasklet(InternalSerializationService serializationService,
                           OutboundCollector collector, int senderCount) {
        this.serializationService = serializationService;
        this.collector = collector;
        this.remainingSenders = senderCount;
    }

    void addPacket(byte[] packetBuf, int offset) {
        incoming.add(new ByteArrayInputStream(packetBuf, offset, packetBuf.length - offset));
    }

    @Override
    public ProgressState call() {
        tracker.reset();
        tryFillInbox();
        for (Object item; (item = inbox.peek()) != null; ) {
            ObjectWithPartitionId itemWithpId = (ObjectWithPartitionId) item;
            if (itemWithpId.getItem() == DONE_ITEM) {
                remainingSenders--;
            } else {
                ProgressState offered = collector.offer(itemWithpId.getItem(), itemWithpId.getPartitionId());
                if (!offered.isDone()) {
                    tracker.madeProgress(offered.isMadeProgress());
                    break;
                }
            }
            tracker.madeProgress();
            inbox.remove();
        }

        if (remainingSenders == 0) {
            tracker.mergeWith(collector.close());
        } else {
            tracker.notDone();
        }
        return tracker.toProgressState();
    }


    private void tryFillInbox() {
        try {
            for (ByteArrayInputStream entry; (entry = incoming.poll()) != null; ) {
                try (ObjectDataInputStream inputStream = new ObjectDataInputStream(entry, serializationService)) {
                    int count = inputStream.readInt();
                    for (int i = 0; i < count; i++) {
                        Object item = inputStream.readObject();
                        int partitionId = inputStream.readInt();
                        inbox.add(new ObjectWithPartitionId(item, partitionId));
                    }
                    tracker.madeProgress();
                }
            }
        } catch (IOException e) {
            throw rethrow(e);
        }
    }
}
