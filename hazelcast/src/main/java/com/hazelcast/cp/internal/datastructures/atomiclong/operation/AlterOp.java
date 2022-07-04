/*
 * Copyright (c) 2008-2022, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.cp.internal.datastructures.atomiclong.operation;

import com.hazelcast.cp.IAtomicLong;
import com.hazelcast.core.IFunction;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.cp.CPGroupId;
import com.hazelcast.cp.internal.datastructures.atomiclong.AtomicLongDataSerializerHook;
import com.hazelcast.cp.internal.datastructures.atomiclong.AtomicLong;

import java.io.IOException;

import static com.hazelcast.cp.internal.datastructures.atomiclong.operation.AlterOp.AlterResultType.OLD_VALUE;
import static com.hazelcast.internal.util.Preconditions.checkNotNull;

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
        OLD_VALUE(0),
        /**
         * The value after the function is applied
         */
        NEW_VALUE(1);

        private final int value;

        AlterResultType(int value) {
            this.value = value;
        }

        public static AlterResultType fromValue(int value) {
            switch (value) {
                case 0:
                    return OLD_VALUE;
                case 1:
                    return NEW_VALUE;
                default:
                    throw new IllegalArgumentException("No " + AlterResultType.class + " for value: " + value);
            }
        }

        public int value() {
            return value;
        }
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
    public int getClassId() {
        return AtomicLongDataSerializerHook.ALTER_OP;
    }

    @Override
    public Object run(CPGroupId groupId, long commitIndex) {
        AtomicLong atomic = getAtomicLong(groupId);
        long before = atomic.getAndAdd(0);
        long after = function.apply(before);
        atomic.getAndSet(after);
        return alterResultType == OLD_VALUE ? before : after;
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        super.writeData(out);
        out.writeObject(function);
        out.writeString(alterResultType.name());
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        super.readData(in);
        function = in.readObject();
        alterResultType = AlterResultType.valueOf(in.readString());
    }

}
