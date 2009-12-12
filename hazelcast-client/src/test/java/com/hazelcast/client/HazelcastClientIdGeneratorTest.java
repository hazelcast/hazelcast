/* 
 * Copyright (c) 2008-2009, Hazel Ltd. All Rights Reserved.
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
 *
 */

package com.hazelcast.client;

import org.junit.After;
import org.junit.Test;
import static org.junit.Assert.assertNull;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.IdGenerator;
import com.hazelcast.core.HazelcastInstance;
import static com.hazelcast.client.TestUtility.getHazelcastClient;
import static junit.framework.Assert.assertTrue;

import java.util.HashMap;
import java.util.Map;

public class HazelcastClientIdGeneratorTest {

     private HazelcastClient hClient;



    @After
    public void shutdownAll() throws InterruptedException{
    	Hazelcast.shutdownAll();
    	if(hClient!=null){	hClient.shutdown(); }
    	Thread.sleep(500);
    }

    @Test

    public void idGenerator(){
        HazelcastInstance h = Hazelcast.newHazelcastInstance(null);
        hClient = getHazelcastClient(h);
        IdGenerator nativeId = h.getIdGenerator("id");
        IdGenerator clientId = hClient.getIdGenerator("id");
        long v = clientId.newId();
        Map map = new HashMap();
        int count = 10;

        for(int i=0;i<count;i++){
           long genId = nativeId.newId();
           assertNull( map.put(genId,1));
            System.out.println(genId);
        }
        System.out.println(v);

        for(int i=0;i<count;i++){
            long genId = clientId.newId();
            assertNull(map.put(genId,1));
            System.out.println(genId);
        }
        assertTrue(true);

    }

}
