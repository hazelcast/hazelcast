/* 
 * Copyright (c) 2008-2010, Hazel Ltd. All Rights Reserved.
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
 *
 */

package com.hazelcast.impl.management;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.HazelcastInstanceAware;
import com.hazelcast.core.Member;
import com.hazelcast.nio.DataSerializable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.concurrent.Callable;

public class ThreadDumpCallable implements Callable<ThreadDumpResult>, DataSerializable, HazelcastInstanceAware {

    private static final long serialVersionUID = -1910495089344606344L;

    private transient HazelcastInstance hazelcastInstance;

    public ThreadDumpResult call() throws Exception {
        ThreadDumpGenerator generator = ThreadDumpGenerator.newInstance();
        String result = generator.dumpAllThreads();
        Member member = hazelcastInstance.getCluster().getLocalMember();
        return new ThreadDumpResult(member, result);
    }

    public void setHazelcastInstance(HazelcastInstance hazelcastInstance) {
        this.hazelcastInstance = hazelcastInstance;
    }

    public void writeData(DataOutput out) throws IOException {
        //nop
    }

    public void readData(DataInput in) throws IOException {
        //nop
    }
}
