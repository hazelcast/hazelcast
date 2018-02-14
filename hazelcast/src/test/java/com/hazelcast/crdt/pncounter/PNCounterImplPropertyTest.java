package com.hazelcast.crdt.pncounter;

import com.hazelcast.crdt.BaseCRDTPropertyTest;
import com.hazelcast.crdt.Operation;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import com.hazelcast.util.MutableLong;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.Arrays;
import java.util.List;
import java.util.Random;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelTest.class})
public class PNCounterImplPropertyTest extends BaseCRDTPropertyTest<PNCounterImpl, MutableLong, Long> {

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
    protected PNCounterImpl getCRDT(int i) {
        return new PNCounterImpl("replica " + i, "counter");
    }
}
