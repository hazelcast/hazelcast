/*
 * Copyright (c) 2008-2015, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.instance;

import com.hazelcast.config.Config;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.HazelcastInstanceAware;
import com.hazelcast.core.ManagedContext;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.TestHazelcastInstanceFactory;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static org.junit.Assert.assertNotNull;

@RunWith(HazelcastParallelClassRunner.class)
@Category(QuickTest.class)
public class ManagedContextInstanceAwareTest extends HazelcastTestSupport {

    @Test
    public void test(){
        Config config = new Config();
        ManagedContextImpl managedContext = new ManagedContextImpl();
        config.setManagedContext(managedContext);

        HazelcastInstance hz = createHazelcastInstance(config);
        assertNotNull("hazelcastInstance should have been set",managedContext.hz);
    }

    private class ManagedContextImpl implements ManagedContext, HazelcastInstanceAware {
        private HazelcastInstance hz;

        @Override
        public void setHazelcastInstance(HazelcastInstance hazelcastInstance) {
            this.hz= hazelcastInstance;
        }

        @Override
        public Object initialize(Object obj) {
            return null;
        }
    }
}
