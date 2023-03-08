package com.hazelcast.pubsub.impl;

import com.hazelcast.internal.tpcengine.file.AsyncFile;
import com.hazelcast.internal.tpcengine.iobuffer.IOBuffer;
import com.hazelcast.internal.tpcengine.util.OS;

public class TopicSegment {

    public static final int SIZEOF_SEGMENT_HEADER = OS.pageSize();

    public long id;
    public IOBuffer header;
    public AsyncFile file;
    public long offset = -1;
    public long fileOffset = -1;
    public IOBuffer buffer;
}
