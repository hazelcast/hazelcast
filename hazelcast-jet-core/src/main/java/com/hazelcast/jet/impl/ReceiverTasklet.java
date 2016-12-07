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

package com.hazelcast.jet.impl;

import com.hazelcast.internal.util.concurrent.MPSCQueue;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.util.concurrent.IdleStrategy;

import java.io.IOException;
import java.util.ArrayDeque;
import java.util.Arrays;
import java.util.Queue;
import java.util.concurrent.atomic.AtomicInteger;

import static com.hazelcast.jet.impl.DoneItem.DONE_ITEM;
import static com.hazelcast.util.ExceptionUtil.rethrow;

/**
 * Receives from all remote members the data associated with a single edge.
 */
class ReceiverTasklet implements Tasklet {

    private final Queue<PacketWithSender> incoming = new MPSCQueue<>((IdleStrategy) null);
    private final ProgressTracker tracker = new ProgressTracker();
    private final ArrayDeque<ObjPtionAndSenderId> inbox = new ArrayDeque<>();
    private final OutboundCollector collector;
    private final AtomicInteger[] itemSeqsPerSender;

    private int remainingSenders;

    ReceiverTasklet(OutboundCollector collector, int senderCount) {
        this.collector = collector;
        this.remainingSenders = senderCount;
        this.itemSeqsPerSender = new AtomicInteger[senderCount];
        Arrays.setAll(itemSeqsPerSender, x -> new AtomicInteger());
    }

    void addPacket(ObjectDataInput packetInput, int senderId) {
        incoming.add(new PacketWithSender(senderId, packetInput));
    }

    int ackedSeq(int senderId) {
        return itemSeqsPerSender[senderId].get();
    }

    @Override
    public ProgressState call() {
        tracker.reset();
        tryFillInbox();
        for (ObjPtionAndSenderId o; (o = inbox.peek()) != null; ) {
            final Object item = o.getItem();
            if (item == DONE_ITEM) {
                remainingSenders--;
            } else {
                ProgressState outcome = collector.offer(item, o.getPartitionId());
                if (!outcome.isDone()) {
                    tracker.madeProgress(outcome.isMadeProgress());
                    break;
                }
            }
            tracker.madeProgress();
            inbox.remove();
            final AtomicInteger seq = itemSeqsPerSender[o.senderId];
            seq.lazySet(seq.get() + 1);
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
            for (PacketWithSender received; (received = incoming.poll()) != null; ) {
                final ObjectDataInput in = received.payload;
                final int itemCount = in.readInt();
                for (int i = 0; i < itemCount; i++) {
                    inbox.add(new ObjPtionAndSenderId(in.readObject(), in.readInt(), received.senderId));
                }
                tracker.madeProgress();
            }
        } catch (IOException e) {
            throw rethrow(e);
        }
    }

    private static class PacketWithSender {
        final int senderId;
        final ObjectDataInput payload;

        PacketWithSender(int senderId, ObjectDataInput payload) {
            this.senderId = senderId;
            this.payload = payload;
        }
    }

    private static class ObjPtionAndSenderId extends ObjectWithPartitionId {
        final int senderId;

        ObjPtionAndSenderId(Object item, int partitionId, int senderId) {
            super(item, partitionId);
            this.senderId = senderId;
        }
    }
}
