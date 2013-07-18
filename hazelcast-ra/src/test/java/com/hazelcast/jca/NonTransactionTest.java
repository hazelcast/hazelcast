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

package com.hazelcast.jca;


import com.hazelcast.core.IList;
import com.hazelcast.core.IMap;
import org.jboss.arquillian.junit.Arquillian;
import org.junit.Test;
import org.junit.runner.RunWith;

import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

/**
 * no transaction tests using connection from container
 *
 * @author asimarslan
 */
@RunWith(Arquillian.class)
public class NonTransactionTest extends AbstractDeploymentTest {



    @Test
    public void testMap() throws Throwable {
        HazelcastConnection c = getConnection();

        IMap<Integer, String> m = c.getMap("testMap");

        Integer key1=1;
        String value1="value1";

        Integer key2=2;
        String value2="value2";

        m.put(key1,value1);
        m.put(key2,value2);


        assertEquals(m.get(key1),value1);
        assertEquals(m.get(key2),value2);

        c.close();
    }

    @Test
    public void testList() throws Throwable  {
        HazelcastConnection c = getConnection();

        final IList<String> testList = c.getList("testList");

        testList.add("item1");
        testList.add("item2");

        assertEquals(2,c.getList("testList").size());

        assertEquals("item1",c.getList("testList").get(0));
        assertEquals("item2",c.getList("testList").get(1));

        c.close();
    }
}
