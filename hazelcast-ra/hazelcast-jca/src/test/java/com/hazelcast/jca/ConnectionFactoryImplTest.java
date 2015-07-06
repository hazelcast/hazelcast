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

package com.hazelcast.jca;

import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import javax.naming.NamingException;
import javax.naming.Reference;
import javax.resource.Referenceable;
import java.io.Serializable;

import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;

@RunWith(HazelcastParallelClassRunner.class)
@Category(QuickTest.class)
public class ConnectionFactoryImplTest extends HazelcastTestSupport {

    private HazelcastConnectionFactory connectionFactory;
    private Reference ref;

    @Before
    public void setup() throws Exception {
        connectionFactory = new ConnectionFactoryImpl();
        ref = new Reference(randomString());
        connectionFactory.setReference(ref);
    }

    @Test
    public void testJNDI() throws NamingException {
        assertSame(ref,connectionFactory.getReference());
        assertTrue(connectionFactory instanceof Referenceable);
        assertTrue(connectionFactory instanceof Serializable);
    }
}
