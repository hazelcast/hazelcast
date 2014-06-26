/*
 * Copyright (c) 2008-2013, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.ascii.memcache;

import com.hazelcast.ascii.AbstractTextCommand;
import com.hazelcast.ascii.TextCommandConstants;
import com.hazelcast.nio.IOUtil;

import java.nio.ByteBuffer;

import static com.hazelcast.util.StringUtil.stringToBytes;

public class StatsCommand extends AbstractTextCommand {

    static final byte[] STAT = stringToBytes("STAT ");
    static final byte[] UPTIME = stringToBytes("uptime ");
    static final byte[] BYTES = stringToBytes("bytes ");
    static final byte[] CMD_SET = stringToBytes("cmdSet ");
    static final byte[] CMD_GET = stringToBytes("cmdGet ");
    static final byte[] CMD_TOUCH = stringToBytes("cmdTouch ");
    static final byte[] THREADS = stringToBytes("threads ");
    static final byte[] WAITING_REQUESTS = stringToBytes("waitingRequests ");
    static final byte[] GET_HITS = stringToBytes("getHits ");
    static final byte[] GET_MISSES = stringToBytes("getMisses ");
    static final byte[] DELETE_HITS = stringToBytes("deleteHits ");
    static final byte[] DELETE_MISSES = stringToBytes("deleteMisses ");
    static final byte[] INCR_HITS = stringToBytes("incrHits ");
    static final byte[] INCR_MISSES = stringToBytes("incrMisses ");
    static final byte[] DECR_HITS = stringToBytes("decrHits ");
    static final byte[] DECR_MISSES = stringToBytes("decrMisses ");
    static final byte[] CURR_CONNECTIONS = stringToBytes("currConnections ");
    static final byte[] TOTAL_CONNECTIONS = stringToBytes("totalConnections ");

    private static final int CAPACITY = 1000;

    ByteBuffer response;

    public StatsCommand() {
        super(TextCommandConstants.TextCommandType.STATS);
    }

    public boolean readFrom(ByteBuffer cb) {
        return true;
    }

    public void setResponse(Stats stats) {
        response = ByteBuffer.allocate(CAPACITY);
        putInt(UPTIME, stats.getUptime());
        putInt(THREADS, stats.getThreads());
        putInt(WAITING_REQUESTS, stats.getWaitingRequests());
        putInt(CURR_CONNECTIONS, stats.getCurrConnections());
        putInt(TOTAL_CONNECTIONS, stats.getTotalConnections());
        putLong(BYTES, stats.getBytes());
        putLong(CMD_GET, stats.getCmdGet());
        putLong(CMD_SET, stats.getCmdSet());
        putLong(CMD_TOUCH, stats.getCmdTouch());
        putLong(GET_HITS, stats.getGetHits());
        putLong(GET_MISSES, stats.getGetMisses());
        putLong(DELETE_HITS, stats.getDeleteHits());
        putLong(DELETE_MISSES, stats.getDeleteMisses());
        putLong(INCR_HITS, stats.getIncrHits());
        putLong(INCR_MISSES, stats.getIncrMisses());
        putLong(DECR_HITS, stats.getDecrHits());
        putLong(DECR_MISSES, stats.getDecrMisses());
        response.put(TextCommandConstants.END);
        response.flip();
    }

    private void putInt(byte[] name, int value) {
        response.put(STAT);
        response.put(name);
        response.put(stringToBytes(String.valueOf(value)));
        response.put(TextCommandConstants.RETURN);
    }

    private void putLong(byte[] name, long value) {
        response.put(STAT);
        response.put(name);
        response.put(stringToBytes(String.valueOf(value)));
        response.put(TextCommandConstants.RETURN);
    }

    public boolean writeTo(ByteBuffer bb) {
        if (response == null) {
            response = ByteBuffer.allocate(0);
        }
        IOUtil.copyToHeapBuffer(response, bb);
        return !response.hasRemaining();
    }

    @Override
    public String toString() {
        return "StatsCommand{"
                + '}' + super.toString();
    }
}
