package com.hazelcast.pubsub.impl;

import com.hazelcast.internal.tpc.AsyncFile;
import com.hazelcast.internal.tpc.iobuffer.IOBuffer;
import com.hazelcast.internal.tpc.util.OS;

import java.io.File;

// https://strimzi.io/blog/2021/12/17/kafka-segment-retention/
public class TopicLog {

    private final int partitionId;
    private final File rootDir;
    private long offset = 0;
    private long activeSegmentId = 0;
    private AsyncFile activeSegment;
    private int maxRetainedSegments = 10;
    private long segmentSize = 1 * 1024 * OS.pageSize();
    private long segmentOffset = -1;
    private IOBuffer tailBuffer;
    private File dir;

    public TopicLog(int partitionId, File rootDir) {
        this.partitionId = partitionId;
        this.rootDir = rootDir;
    }

    public File getRootDir() {
        return rootDir;
    }

    public File getDir() {
        return dir;
    }

    public void setDir(File dir) {
        this.dir = dir;
    }

    public IOBuffer getActiveBuffer() {
        return tailBuffer;
    }

    public void setActiveBuffer(IOBuffer tailBuffer) {
        this.tailBuffer = tailBuffer;
    }

    public int getMaxRetainedSegments() {
        return maxRetainedSegments;
    }

    public long segmentOffset() {
        return segmentOffset;
    }

    public void segmentOffset(long offset) {
        this.segmentOffset = offset;
    }

    public void incOffset(long amount) {
        offset += amount;
    }

    public long offset() {
        return offset;
    }

    public long incSegmentOffset(long amount) {
        segmentOffset += amount;
        return segmentOffset;
    }

    public long getActiveSegmentId() {
        return activeSegmentId;
    }

    public long getNextActiveSegmentId() {
        return activeSegmentId++;
    }

    public void setActiveSegment(AsyncFile activeSegment) {
        this.activeSegment = activeSegment;
    }

    public long getSegmentSize() {
        return segmentSize;
    }

    public AsyncFile getActiveSegment() {
        return activeSegment;
    }

    public int getPartitionId() {
        return partitionId;
    }
}
