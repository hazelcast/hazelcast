/*
 * Copyright (c) 2008-2018, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.raft.service.atomiclong.operation;

import com.hazelcast.core.IAtomicLong;
import com.hazelcast.core.IFunction;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.raft.RaftGroupId;
import com.hazelcast.raft.service.atomiclong.RaftAtomicLongDataSerializerHook;
import com.hazelcast.raft.service.atomiclong.RaftAtomicLong;

import java.io.IOException;

import static com.hazelcast.raft.service.atomiclong.operation.AlterOp.AlterResultType.BEFORE_VALUE;
import static com.hazelcast.util.Preconditions.checkNotNull;

/**
 * Operation for {@link IAtomicLong#alter(IFunction)}
 */
public class AlterOp extends AbstractAtomicLongOp {

    /**
     * Denotes how the return value will be chosen.
     */
    public enum AlterResultType {
        /**
         * The value before the function is applied
         */
        BEFORE_VALUE,
        /**
         * The value after the function is applied
         */
        AFTER_VALUE
    }

    private IFunction<Long, Long> function;
    private AlterResultType alterResultType;

    public AlterOp() {
    }

    public AlterOp(String name, IFunction<Long, Long> function, AlterResultType alterResultType) {
        super(name);
        checkNotNull(alterResultType);
        this.function = function;
        this.alterResultType = alterResultType;
    }

    @Override
    public int getId() {
        return RaftAtomicLongDataSerializerHook.ALTER_OP;
    }

    @Override
    public Object run(RaftGroupId groupId, long commitIndex) {
        RaftAtomicLong atomic = getAtomicLong(groupId);
        long before = atomic.getAndAdd(0);
        long after = function.apply(before);
        atomic.getAndSet(after);
        return alterResultType == BEFORE_VALUE ? before : after;
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        super.writeData(out);
        out.writeObject(function);
        out.writeUTF(alterResultType.name());
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        super.readData(in);
        function = in.readObject();
        alterResultType = AlterResultType.valueOf(in.readUTF());
    }

}
