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

package com.hazelcast.nio.serialization;

import com.hazelcast.core.EntryEventType;
import com.hazelcast.test.HazelcastJUnit4ClassRunner;
import com.hazelcast.test.annotation.ParallelTest;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.io.IOException;
import java.lang.annotation.RetentionPolicy;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertSame;

@RunWith(HazelcastJUnit4ClassRunner.class)
@Category(ParallelTest.class)
public class EnumTest {

    final SerializationService ss = new SerializationServiceBuilder().build();

    @Test
    public void test1() throws IOException {
        test(EntryEventType.ADDED);
    }

    @Test
    public void test2() throws IOException {
        test(Thread.State.RUNNABLE);
    }

    @Test
    public void test3() throws IOException {
        test(RetentionPolicy.SOURCE);
    }

    @Test
    @Ignore
    // TimeUnit.SECONDS.getClass().isEnum() returns false!
    public void test4() throws IOException {
        test(TimeUnit.SECONDS);
    }

    private void test(Enum value) throws IOException {
        Data data = ss.toData(value);
        Enum found = (Enum) ss.toObject(data);
        assertSame(value, found);
    }
}
