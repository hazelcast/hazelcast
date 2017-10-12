/*
 * Copyright (c) 2008-2017, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.spi.impl.eventservice.impl.operations;

import com.hazelcast.spi.OperationFactory;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelTest.class})
public class DeregistrationOperationFactoryTest {

    private OperationFactory factory = new DeregistrationOperationFactory();

    @Test(expected = UnsupportedOperationException.class)
    public void testFactoryId() {
        factory.getFactoryId();
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testGetId() {
        factory.getId();
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testWriteData() throws Exception {
        factory.writeData(null);
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testReadData() throws Exception {
        factory.readData(null);
    }
}
