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

package com.hazelcast.internal.crdt.pncounter;

import com.hazelcast.internal.crdt.AbstractCRDTPropertyTest;
import com.hazelcast.internal.crdt.Operation;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import com.hazelcast.internal.util.MutableLong;
import com.hazelcast.internal.util.UuidUtil;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.Arrays;
import java.util.List;
import java.util.Random;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class PNCounterImplPropertyTest extends AbstractCRDTPropertyTest<PNCounterImpl, MutableLong, Long> {

    @Override
    protected MutableLong getStateHolder() {
        return new MutableLong();
    }

    @Override
    protected Long getStateValue(PNCounterImpl t) {
        return t.get(null).getValue();
    }

    @Override
    protected Long getStateValue(MutableLong t) {
        return t.value;
    }

    public static class AddAndGet extends Operation<PNCounterImpl, MutableLong> {
        private final int delta;

        public AddAndGet(Random rnd) {
            super(rnd);
            delta = rnd.nextInt(200) - 100;
        }

        @Override
        protected void perform(PNCounterImpl crdt, MutableLong state) {
            crdt.addAndGet(delta, null);
            state.value += delta;
        }

        @Override
        public String toString() {
            return "AddAndGet(" + crdtIndex + "," + delta + ")";
        }
    }

    public static class GetAndAdd extends Operation<PNCounterImpl, MutableLong> {
        private final int delta;

        public GetAndAdd(Random rnd) {
            super(rnd);
            delta = rnd.nextInt(200) - 100;
        }

        @Override
        protected void perform(PNCounterImpl crdt, MutableLong state) {
            crdt.getAndAdd(delta, null);
            state.value += delta;
        }

        @Override
        public String toString() {
            return "GetAndAdd(" + crdtIndex + "," + delta + ")";
        }
    }

    public static class GetAndSubtract extends Operation<PNCounterImpl, MutableLong> {
        private final int delta;

        public GetAndSubtract(Random rnd) {
            super(rnd);
            delta = rnd.nextInt(200) - 100;
        }

        @Override
        protected void perform(PNCounterImpl crdt, MutableLong state) {
            crdt.getAndSubtract(delta, null);
            state.value -= delta;
        }

        @Override
        public String toString() {
            return "GetAndSubtract(" + crdtIndex + "," + delta + ")";
        }
    }

    public static class SubtractAndGet extends Operation<PNCounterImpl, MutableLong> {
        private final int delta;

        public SubtractAndGet(Random rnd) {
            super(rnd);
            this.delta = rnd.nextInt(200) - 100;
        }

        @Override
        protected void perform(PNCounterImpl crdt, MutableLong state) {
            crdt.subtractAndGet(delta, null);
            state.value -= delta;
        }

        @Override
        public String toString() {
            return "SubtractAndGet(" + crdtIndex + "," + delta + ")";
        }
    }

    @Override
    @SuppressWarnings("unchecked")
    protected List<Class<? extends Operation<PNCounterImpl, MutableLong>>> getOperationClasses() {
        return Arrays.asList(AddAndGet.class, GetAndAdd.class, GetAndSubtract.class, SubtractAndGet.class);
    }

    @Override
    protected PNCounterImpl getCRDT() {
        return new PNCounterImpl(UuidUtil.newSecureUUID(), "counter");
    }
}
