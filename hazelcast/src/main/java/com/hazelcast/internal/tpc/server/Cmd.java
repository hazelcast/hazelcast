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

package com.hazelcast.internal.tpc.server;

import com.hazelcast.internal.tpcengine.Eventloop;
import com.hazelcast.internal.tpcengine.iobuffer.IOBuffer;

/**
 * a {@link Cmd} is comparable with the classic Hazelcast Operation.
 *
 * Configure Concurrency at Cmd level? E.g. some operations could require exclusive
 * access while other could allow for concurrent commands.
 */
public abstract class Cmd {

    public final static int COMPLETED = 0;
    public final static int BLOCKED = 1;
    public final static int EXCEPTION = 2;

    public boolean priority;
    public int partitionId;
    public long callId;
    public ManagerRegistry managerRegistry;
    // The id that uniquely identifies a command.
    public final int id;
    public IOBuffer request;
    public IOBuffer response;
    public CmdAllocator allocator;
    public RequestScheduler scheduler;
    public Eventloop eventloop;

    public Cmd(int id) {
        this.id = id;
    }

    /**
     * Is only called once during the whole lifetime of the Cmd.
     */
    public void init(){
    }

    public abstract int run() throws Exception;

    /**
     * Will be run after every run that doesn't block.
     */
    public void clear() {
    }

    public void release() {
        if (allocator != null) {
            allocator.free(this);
        }
    }
}
