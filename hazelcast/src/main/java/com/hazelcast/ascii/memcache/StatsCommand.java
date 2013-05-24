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

public class StatsCommand extends AbstractTextCommand {
    ByteBuffer response;
    static final byte[] STAT = "STAT ".getBytes();
    static final byte[] UPTIME = "uptime ".getBytes();
    static final byte[] BYTES = "bytes ".getBytes();
    static final byte[] CMD_SET = "cmd_set ".getBytes();
    static final byte[] CMD_GET = "cmd_get ".getBytes();
    static final byte[] CMD_TOUCH = "cmd_touch ".getBytes();
    static final byte[] THREADS = "threads ".getBytes();
    static final byte[] WAITING_REQUESTS = "waiting_requests ".getBytes();
    static final byte[] GET_HITS = "get_hits ".getBytes();
    static final byte[] GET_MISSES = "get_misses ".getBytes();
    static final byte[] DELETE_HITS = "delete_hits ".getBytes();
    static final byte[] DELETE_MISSES = "delete_misses ".getBytes();
    static final byte[] INCR_HITS = "incr_hits ".getBytes();
    static final byte[] INCR_MISSES = "incr_misses ".getBytes();
    static final byte[] DECR_HITS = "decr_hits ".getBytes();
    static final byte[] DECR_MISSES = "decr_misses ".getBytes();
    static final byte[] CURR_CONNECTIONS = "curr_connections ".getBytes();
    static final byte[] TOTAL_CONNECTIONS = "total_connections ".getBytes();

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
        response.put(String.valueOf(value).getBytes());
        response.put(RETURN);
    }

    private void putLong(byte[] name, long value) {
        response.put(STAT);
        response.put(name);
        response.put(String.valueOf(value).getBytes());
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
        return "StatsCommand{" +
                '}' + super.toString();
    }
}
