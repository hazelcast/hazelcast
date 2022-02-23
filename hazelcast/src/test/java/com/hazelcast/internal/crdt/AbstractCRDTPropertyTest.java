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

package com.hazelcast.internal.crdt;

import org.junit.Before;
import org.junit.Test;

import java.lang.reflect.Constructor;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Random;

import static org.junit.Assert.assertArrayEquals;

public abstract class AbstractCRDTPropertyTest<C extends CRDT<C>, H, S> {

    private final Random random = new Random();
    private int numCounters;

    @Before
    public void init() {
        this.numCounters = 2 + random.nextInt(3);
    }

    @Test
    public void strongEventualConsistency() {
        final int rounds = 1000;
        for (int i = 0; i < rounds; i++) {
            final int operationCount = 50 + random.nextInt(50);
            final List<C> crdts = setupCRDTs(numCounters);
            final H state = getStateHolder();
            final Operation<C, H>[] operations = generateOperations(operationCount);

            for (Operation<C, H> operation : operations) {
                operation.perform(crdts, state);
            }
            mergeAll(crdts);
            assertState(crdts, state, numCounters, operations);
        }
    }

    @SuppressWarnings("unchecked")
    private Operation<C, H>[] generateOperations(int operationCount) {
        final List<Class<? extends Operation<C, H>>> operationClasses = getOperationClasses();
        final int size = random.nextInt(operationCount);
        final Operation[] operations = new Operation[size];
        for (int i = 0; i < size; i++) {
            if (random.nextDouble() < 0.1) {
                operations[i] = new MergingOperation(random);
            } else {
                try {
                    final Class operationClass = operationClasses.get(random.nextInt(operationClasses.size()));
                    final Constructor<Operation> c = operationClass.getConstructor(Random.class);
                    operations[i] = c.newInstance(random);
                } catch (Exception e) {
                    throw new RuntimeException("Error instantiating the operation ", e);
                }
            }
        }

        return operations;
    }

    protected abstract List<Class<? extends Operation<C, H>>> getOperationClasses();

    protected abstract C getCRDT();

    protected abstract H getStateHolder();

    protected abstract S getStateValue(C t);

    protected abstract S getStateValue(H t);

    protected void assertState(List<C> crdts, H stateHolder, int counterCount, Operation<C, H>[] operations) {
        final Object[] actuals = new Object[crdts.size()];
        final Object[] expecteds = new Object[crdts.size()];
        for (int i = 0; i < crdts.size(); i++) {
            actuals[i] = getStateValue(crdts.get(i));
            expecteds[i] = getStateValue(stateHolder);
        }
        assertArrayEquals("States differ for " + counterCount + " with operations " + Arrays.toString(operations),
                expecteds, actuals);
    }

    private List<C> setupCRDTs(int size) {
        final List<C> counters = new ArrayList<C>(size);
        for (int i = 0; i < size; i++) {
            counters.add(getCRDT());
        }
        return counters;
    }

    private void mergeAll(List<C> crdts) {
        for (C c1 : crdts) {
            for (C c2 : crdts) {
                if (c1 != c2) {
                    c1.merge(c2);
                }
            }
        }
    }
}
