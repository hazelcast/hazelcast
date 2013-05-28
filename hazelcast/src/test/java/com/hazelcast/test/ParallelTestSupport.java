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

package com.hazelcast.test;

import com.hazelcast.test.annotation.OperationalTest;
import org.junit.After;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

/**
 * @mdogan 5/24/13
 */

@RunWith(RandomBlockJUnit4ClassRunner.class)
@Category(OperationalTest.class)
public abstract class ParallelTestSupport {

    private StaticNodeFactory factory;

    protected final StaticNodeFactory createNodeFactory(int nodeCount) {
        if (factory != null) {
            throw new IllegalStateException("Node factory is already created!");
        }
        return factory = new StaticNodeFactory(nodeCount);
    }

    @After
    public final void shutdownNodeFactory() {
        final StaticNodeFactory f = factory;
        if (f != null) {
            factory = null;
            f.shutdownAll();
        }
    }
}
