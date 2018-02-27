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

package com.hazelcast.jet.impl.connector;

import com.hazelcast.jet.IListJet;
import com.hazelcast.jet.JetInstance;
import com.hazelcast.jet.core.JetTestSupport;
import com.hazelcast.jet.core.test.TestSupport;
import com.hazelcast.test.HazelcastParallelClassRunner;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;

import java.util.List;
import java.util.stream.IntStream;

import static java.util.stream.Collectors.toList;

@RunWith(HazelcastParallelClassRunner.class)
public class ReadIListPTest extends JetTestSupport {

    private JetInstance instance;

    @Before
    public void setUp() {
        instance = this.createJetMember();
    }

    @Test
    public void when_sizeLessThanFetchSize_then_readAll() {
        testReader(ReadIListP.FETCH_SIZE / 2);
    }

    @Test
    public void when_sizeMoreThanFetchSize_then_readAll() {
        testReader(ReadIListP.FETCH_SIZE * 3 / 2);
    }

    private void testReader(int listLength) {
        IListJet<Object> list = instance.getList(randomName());
        List<Object> data = IntStream.range(0, listLength).boxed().collect(toList());
        list.addAll(data);
        TestSupport
                .verifyProcessor(new ReadIListP(list.getName(), null))
                .jetInstance(instance)
                .disableSnapshots()
                .disableLogging()
                .expectOutput(data);
    }
}
