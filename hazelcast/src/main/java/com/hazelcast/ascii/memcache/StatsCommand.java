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
import com.hazelcast.nio.IOUtil;

import java.nio.ByteBuffer;

import static com.hazelcast.util.StringUtil.stringToBytes;

public class StatsCommand extends AbstractTextCommand {

    static final byte[] STAT = stringToBytes("STAT ");
    static final byte[] UPTIME = stringToBytes("uptime ");
    static final byte[] BYTES = stringToBytes("bytes ");
    static final byte[] CMD_SET = stringToBytes("cmd_set ");
    static final byte[] CMD_GET = stringToBytes("cmd_get ");
    static final byte[] CMD_TOUCH = stringToBytes("cmd_touch ");
    static final byte[] THREADS = stringToBytes("threads ");
    static final byte[] WAITING_REQUESTS = stringToBytes("waiting_requests ");
    static final byte[] GET_HITS = stringToBytes("get_hits ");
    static final byte[] GET_MISSES = stringToBytes("get_misses ");
    static final byte[] DELETE_HITS = stringToBytes("delete_hits ");
    static final byte[] DELETE_MISSES = stringToBytes("delete_misses ");
    static final byte[] INCR_HITS = stringToBytes("incr_hits ");
    static final byte[] INCR_MISSES = stringToBytes("incr_misses ");
    static final byte[] DECR_HITS = stringToBytes("decr_hits ");
    static final byte[] DECR_MISSES = stringToBytes("decr_misses ");
    static final byte[] CURR_CONNECTIONS = stringToBytes("curr_connections ");
    static final byte[] TOTAL_CONNECTIONS = stringToBytes("total_connections ");
    ByteBuffer response;

    public StatsCommand() {
        super(TextCommandType.STATS);
    }

    public boolean readFrom(ByteBuffer cb) {
        return true;
    }

    public void setResponse(Stats stats) {
        response = ByteBuffer.allocate(1000);
        putInt(UPTIME, stats.uptime);
        putInt(THREADS, stats.threads);
        putInt(WAITING_REQUESTS, stats.waiting_requests);
        putInt(CURR_CONNECTIONS, stats.curr_connections);
        putInt(TOTAL_CONNECTIONS, stats.total_connections);
        putLong(BYTES, stats.bytes);
        putLong(CMD_GET, stats.cmd_get);
        putLong(CMD_SET, stats.cmd_set);
        putLong(CMD_TOUCH, stats.cmd_touch);
        putLong(GET_HITS, stats.get_hits);
        putLong(GET_MISSES, stats.get_misses);
        putLong(DELETE_HITS, stats.delete_hits);
        putLong(DELETE_MISSES, stats.delete_misses);
        putLong(INCR_HITS, stats.incr_hits);
        putLong(INCR_MISSES, stats.incr_misses);
        putLong(DECR_HITS, stats.decr_hits);
        putLong(DECR_MISSES, stats.decr_misses);
        response.put(END);
        response.flip();
    }

    private void putInt(byte[] name, int value) {
        response.put(STAT);
        response.put(name);
        response.put(stringToBytes(String.valueOf(value)));
        response.put(RETURN);
    }

    private void putLong(byte[] name, long value) {
        response.put(STAT);
        response.put(name);
        response.put(stringToBytes(String.valueOf(value)));
        response.put(RETURN);
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
