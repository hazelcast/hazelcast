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

package com.hazelcast.mapreduce.impl.operation;

import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelTest.class})
public class GetResultOperationFactoryTest {

    private GetResultOperationFactory factory;

    @Before
    public void setUp() {
        factory = new GetResultOperationFactory("name", "jobId");
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testWriteData() throws Exception {
        factory.writeData(null);
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testReadData() throws Exception {
        factory.readData(null);
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testGetFactoryId() {
        factory.getFactoryId();
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testGetId() {
        factory.getId();
    }
}
