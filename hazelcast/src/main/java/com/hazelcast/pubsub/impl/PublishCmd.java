/*
 * Copyright (c) 2008-2023, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.pubsub.impl;

import com.hazelcast.internal.tpc.server.Cmd;
import com.hazelcast.internal.tpc.util.CharVector;
import com.hazelcast.internal.tpcengine.Promise;
import com.hazelcast.internal.tpcengine.file.AsyncFile;
import com.hazelcast.internal.tpcengine.iobuffer.IOBuffer;

import java.io.File;

import static com.hazelcast.internal.tpcengine.file.AsyncFile.O_CREAT;
import static com.hazelcast.internal.tpcengine.file.AsyncFile.O_DIRECT;
import static com.hazelcast.internal.tpcengine.file.AsyncFile.O_RDWR;
import static com.hazelcast.internal.tpcengine.file.AsyncFile.PERMISSIONS_ALL;
import static com.hazelcast.internal.tpcengine.util.BufferUtil.addressOf;
import static com.hazelcast.pubsub.impl.TopicSegment.SIZEOF_SEGMENT_HEADER;


// todo: what happens when an active segment is being made and a concurrent write is performed.
public class PublishCmd extends Cmd {

    public static final byte ID = 7;

    private static final int STATE_INIT = 0;
    private static final int STATE_CREATING_ACTIVE_SEGMENT = 1;
    private static final int STATE_RUN = 2;

    // the topicName is backed by the request.
    private final CharVector topicName = new CharVector();
    private int state = STATE_INIT;
    private byte syncOption;
    private int totalSize;
    private int messageCount;
    private TopicDataManager topicManager;
    private TopicPartition topicPartition;

    public PublishCmd() {
        super(ID);
    }

    @Override
    public void clear() {
        super.clear();
        state = STATE_INIT;
    }

    @Override
    public void init() {
        super.init();
        topicPartition = null;
        topicManager = managerRegistry.get(TopicDataManager.class);
    }

    @Override
    public int run() throws Exception {
        for (; ; ) {
            switch (state) {
                case STATE_INIT: {
                    deserialize();

                    topicPartition = topicManager.getOrCreateTopicPartition(topicName, partitionId);

                    if (topicPartition.activeSegment == null) {
                        roll(topicPartition);
                        state = STATE_CREATING_ACTIVE_SEGMENT;
                        return BLOCKED;
                    }

                    if (topicPartition.activeSegment.buffer == null) {
                        // There is no active buffer, so lets open it.
                        topicPartition.activeSegment.buffer = eventloop
                                .fileIOBufferAllocator()
                                .allocate(topicPartition.activeBufferLength);
                    }

                    state = STATE_RUN;
                }
                break;
                case STATE_RUN:
                    if (topicPartition.activeSegment.offset + totalSize > topicPartition.segmentSize) {
                        // the segment is full, so lets create a new one.
                        // todo: what about pending requests in the pipeline.
                        topicPartition.activeSegment.file.close();
                        //partition.activeSegment.file
                        roll(topicPartition);
                        state = STATE_CREATING_ACTIVE_SEGMENT;
                        return BLOCKED;
                    } else {
                        IOBuffer diskBuffer = topicPartition.activeSegment.buffer;
                        for (; ; ) {
                            diskBuffer.write(request, diskBuffer.remaining());
                            if (diskBuffer.remaining() == 0) {
                                // the disk buffer is filled up, so we can asynchronously write it to disk.
                                IOBuffer oldDiskBuffer = diskBuffer;
                                diskBuffer = eventloop.fileIOBufferAllocator().allocate();
                                topicPartition.activeSegment.buffer = diskBuffer;
                                asyncWrite(oldDiskBuffer);
                            }

                            if (request.remaining() == 0) {
                                // everything got written.
                                break;
                            }
                        }
                        topicPartition.activeSegment.offset += totalSize;
                        return COMPLETED;
                    }
                case STATE_CREATING_ACTIVE_SEGMENT:
                    // the segment has been created, so lets try to run
                    state = STATE_RUN;
                    break;
                default:
                    throw new IllegalStateException("Unknown state " + state);
            }
        }
    }

    private void deserialize() {
        topicName.deserialize(request);

        this.syncOption = request.read();
        this.messageCount = request.readInt();
        //System.out.println("messageCount:" + messageCount);
        this.totalSize = request.readInt();
        //System.out.println("totalMessageSize:" + totalMessageSize);
    }

    private void asyncWrite(IOBuffer diskBuffer) {
        int length = diskBuffer.capacity();
        long address = addressOf(diskBuffer.byteBuffer());

        Promise<Integer> pwrite = topicPartition.activeSegment.file
                .pwrite(topicPartition.activeSegment.fileOffset, length, address);
        topicPartition.activeSegment.fileOffset += length;
        pwrite.releaseOnComplete();
        // todo: creating object
        pwrite.then((result, error) -> {
            if (error != null) {
                throw new RuntimeException(error);
            }

            diskBuffer.release();
        });
    }

    private void roll(TopicPartition topicPartition) {
        long activeSegmentId = ++topicPartition.activeSegmentId;
        String path = topicPartition.dir.getAbsolutePath() + "/" + String.format("%019d", activeSegmentId) + ".log";
        AsyncFile segmentFile = eventloop.newAsyncFile(path);

        Promise<Integer> open = segmentFile.open(O_CREAT | O_DIRECT | O_RDWR, PERMISSIONS_ALL);
        //open.releaseOnComplete();
        open.then((result, error) -> {
            if (error != null) {
                throw new RuntimeException(error);
            }
//
//            Promise<Integer> fallocate = segmentFile.fallocate(0, 0, topicPartition.segmentSize);
//            //fallocate.releaseOnComplete();
//            fallocate.then((result1, error1) -> {
//                if (error1 != null) {
//                    throw new RuntimeException(error1);
//                }

            // get rid of oldest segment
            // this is blocking which sucks.
            long nr = topicPartition.activeSegmentId - topicPartition.maxRetainedSegments;
            File heaFile = new File(topicPartition.dir, String.format("%019d", nr) + ".log");
            heaFile.delete();

            TopicSegment segment = new TopicSegment();//litter
            segment.file = segmentFile;
            topicPartition.activeSegment = segment;
            topicPartition.activeSegment.offset = SIZEOF_SEGMENT_HEADER;
            topicPartition.activeSegment.fileOffset = SIZEOF_SEGMENT_HEADER;
            topicPartition.activeSegment.buffer = eventloop.fileIOBufferAllocator().allocate(topicPartition.activeBufferLength);
            scheduler.schedule(PublishCmd.this);
            //    });
        });
    }
}
