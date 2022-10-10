package com.hazelcast.pubsub.impl;

import com.hazelcast.internal.alto.FrameCodec;
import com.hazelcast.internal.alto.Op;
import com.hazelcast.internal.tpc.AsyncFile;
import com.hazelcast.internal.tpc.Promise;
import com.hazelcast.internal.tpc.iobuffer.IOBuffer;
import com.hazelcast.table.impl.TopicManager;

import java.io.File;

import static com.hazelcast.internal.alto.OpCodes.TOPIC_PUBLISH;
import static com.hazelcast.internal.nio.Bits.INT_SIZE_IN_BYTES;
import static com.hazelcast.internal.tpc.AsyncFile.O_CREAT;
import static com.hazelcast.internal.tpc.AsyncFile.O_DIRECT;
import static com.hazelcast.internal.tpc.AsyncFile.O_RDWR;
import static com.hazelcast.internal.tpc.AsyncFile.PERMISSIONS_ALL;
import static com.hazelcast.internal.tpc.util.BufferUtil.addressOf;
import static com.hazelcast.internal.tpc.util.OS.pageSize;


public class TopicPublishOp extends Op {

    private final StringBuffer topicName = new StringBuffer();
    private byte syncOption;
    private int messageSize;
    private int payloadSize;

    public TopicPublishOp() {
        super(TOPIC_PUBLISH);
    }

    @Override
    public void clear() {
        super.clear();
        messageSize = -1;
    }

    @Override
    public int run() throws Exception {
        //Thread.sleep(1000);

        TopicManager topicManager = managers.topicManager;
        TopicLog topicLog = topicManager.getLog(partitionId);
        AsyncFile activeSegment = topicLog.getActiveSegment();

        if (messageSize == -1) {
            loadRequest();
        }

        if (topicLog.getDir() == null) {
            makeTopicLogDir(topicLog);
        }

        IOBuffer activeBuffer = topicLog.getActiveBuffer();
        if (activeBuffer == null) {
            activeBuffer = eventloop.fileIOBufferAllocator().allocate();
            topicLog.setActiveBuffer(activeBuffer);
        }

        if (activeSegment == null) {
            //System.out.println("No active segment");
            asyncCreateActiveSegment(topicLog);
            return BLOCKED;
        } else if (topicLog.segmentOffset() == -1) {
            //System.out.println("Active segment exist, but not yet intialized");
            // the active segment is being opened, we need to wait
            // todo: we should have a future or something to wait for.
            return BLOCKED;
        } else if (topicLog.segmentOffset() + messageSize > topicLog.getSegmentSize()) {
            runSegmentFull(topicLog, activeSegment);
            return BLOCKED;
        } else {
            runSegmentHasSpace(topicLog, activeSegment, activeBuffer);
            return COMPLETED;
        }
    }

    private static void makeTopicLogDir(TopicLog topicLog) {
        // These call block, but it will only happens once per data-structure per partition. So for now it is ok.
        File dir = new File(topicLog.getRootDir(), Integer.toString(topicLog.getPartitionId()));
        if (!dir.exists()) {
            dir.mkdir();
        }
        topicLog.setDir(dir);
    }

    private void loadRequest() {
        this.syncOption = request.read();
        this.payloadSize = request.readInt();

        // we need to fix this so the payload is written over multiple pages.
        if (payloadSize > pageSize()) {
            throw new RuntimeException("Payload too big. Found " + payloadSize + " max:" + pageSize());
        }
        this.messageSize = payloadSize + INT_SIZE_IN_BYTES;
    }

    private void runSegmentHasSpace(TopicLog topicLog, AsyncFile activeSegment, IOBuffer activeBuffer) {
        //System.out.println("Segment has space");
        IOBuffer oldActiveBuffer = null;
        if (activeBuffer.remaining() < messageSize) {
            // we need to move the segmentOffset forward so that the next write starts at the beginning of the page
            int gap = (int) (topicLog.segmentOffset() % activeBuffer.capacity());
            topicLog.segmentOffset(topicLog.segmentOffset() + gap);
            topicLog.incOffset(gap);

            //   System.out.println("Does not fit in existing buffer");
            //tailbuffer is full.
            //todo: we are currently creating a gap.
            oldActiveBuffer = activeBuffer;
            activeBuffer = scheduler.getEventloop().fileIOBufferAllocator().allocate();
            topicLog.setActiveBuffer(activeBuffer);
        }

        activeBuffer.writeInt(payloadSize);
        activeBuffer.write(request.byteBuffer(), payloadSize);
        topicLog.incOffset(payloadSize);

        if (oldActiveBuffer != null) {
            asyncFlush(activeSegment, topicLog.segmentOffset(), oldActiveBuffer);
            topicLog.incSegmentOffset(oldActiveBuffer.capacity());
        }

        response.writeLong(topicLog.offset());
    }

    private void runSegmentFull(TopicLog topicLog, AsyncFile activeSegment) {
        // the active segment is correct initialized, but there is no space for the message
        //System.out.println("Active segment is full");
        // todo: we are dealing with the future.
        activeSegment.close();
        topicLog.setActiveSegment(null);
        topicLog.segmentOffset(-1);
        asyncCreateActiveSegment(topicLog);
    }

    private void asyncFlush(AsyncFile activeSegment, long offset, IOBuffer diskBuffer) {
        int length = diskBuffer.capacity();
        long address = addressOf(diskBuffer.byteBuffer());

        Promise<Integer> pwrite = activeSegment.pwrite(offset, length, address);
        pwrite.releaseOnComplete();
        pwrite.then((result, error) -> {
            if (error != null) {
                throw new RuntimeException(error);
            }

            diskBuffer.release();
        });
    }

    private void asyncCreateActiveSegment(TopicLog topicLog) {
        String path = topicLog.getDir().getAbsolutePath() + "/" + topicLog.getNextActiveSegmentId() + ".log";
        AsyncFile activeSegment = eventloop.newAsyncFile(path);
        topicLog.setActiveSegment(activeSegment);

        Promise<Integer> open = activeSegment.open(O_CREAT | O_DIRECT | O_RDWR, PERMISSIONS_ALL);
        //open.releaseOnComplete();
        open.then((result, error) -> {
            if (error != null) {
                throw new RuntimeException(error);
            }

            Promise<Integer> fallocate = activeSegment.fallocate(0, 0, topicLog.getSegmentSize());
            //fallocate.releaseOnComplete();
            fallocate.then((result1, error1) -> {
                if (error1 != null) {
                    throw new RuntimeException(error1);
                }

                topicLog.segmentOffset(0);
                scheduler.schedule(TopicPublishOp.this);
            });
        });

        //  this is blocking which sucks.
        long nr = topicLog.getActiveSegmentId() - topicLog.getMaxRetainedSegments();
        File heaFile = new File(topicLog.getDir(), nr + ".log");
        heaFile.delete();
    }
}
