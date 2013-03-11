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

public class Stats {
    public int waiting_requests;
    public int threads;
    public int uptime; //seconds
    public long cmd_get;
    public long cmd_set;
    public long cmd_touch;
    public long get_hits;
    public long get_misses;
    public long delete_hits;
    public long delete_misses;
    public long incr_hits;
    public long incr_misses;
    public long decr_hits;
    public long decr_misses;
    public long bytes;
    public int curr_connections;
    public int total_connections;

//    public Stats(int uptime, int threads, long get_misses, long get_hits, long cmd_set, long cmd_get, long bytes) {
//        this.uptime = uptime;
//        this.threads = threads;
//        this.get_misses = get_misses;
//        this.get_hits = get_hits;
//        this.cmd_set = cmd_set;
//        this.cmd_get = cmd_get;
//        this.bytes = bytes;
//    }

    public Stats() {
    }
}