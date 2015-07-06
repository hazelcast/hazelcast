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
import org.junit.Ignore;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import javax.resource.ResourceException;
import javax.resource.spi.ConnectionRequestInfo;
import javax.resource.spi.ManagedConnection;
import javax.resource.spi.ManagedConnectionFactory;
import java.util.Arrays;
import java.util.HashSet;

import static org.junit.Assert.*;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@RunWith(HazelcastParallelClassRunner.class)
@Category(QuickTest.class)
public class ManagedConnectionFactoryImplTest extends HazelcastTestSupport {

    private ManagedConnectionFactory connectionFactory;

    @Before
    public void setup() throws Exception {
        connectionFactory = new ManagedConnectionFactoryImpl();
    }

    @Test
    public void testSetConnectionTracingEvents() {
        ManagedConnectionFactoryImpl fact = (ManagedConnectionFactoryImpl)connectionFactory;
        String controlStr = "DESTROY,  TX_START  , CLEANUP,TX_COMPLETE";

        fact.setConnectionTracingEvents(controlStr);
        String[] returnItems = delimitedStringToArray(fact.getConnectionTracingEvents(), ",");
        String[] controlItems = delimitedStringToArray(controlStr, ",");

        Arrays.sort(controlItems);
        Arrays.sort(returnItems);

        assertArrayEquals(controlItems, returnItems);
    }

    @Test
    public void testMatchExistingManagedConnection() throws ResourceException {
        ResourceAdapterImpl mockResourceAdapter = mock(ResourceAdapterImpl.class);
        when(mockResourceAdapter.getHazelcastInstance()).thenReturn(null);
        ((ManagedConnectionFactoryImpl)connectionFactory).setResourceAdapter(mockResourceAdapter);

        ((ManagedConnectionFactoryImpl)connectionFactory).setConnectionTracingEvents(null);

        HashSet<ManagedConnection> managedConnections = new HashSet<ManagedConnection>();
        for (int i=0; i < 9; i++) {
            ConnectionRequestInfo cxInfo = mock(ConnectionRequestInfo.class);
            managedConnections.add(connectionFactory.createManagedConnection(null,cxInfo));
        }
        ManagedConnection nullSecurityConnection = connectionFactory.createManagedConnection(null, null);
        managedConnections.add(nullSecurityConnection);

        ManagedConnection retConnection = connectionFactory.matchManagedConnections(managedConnections, null, null);
        assertSame(retConnection, nullSecurityConnection);
    }

    private String[] delimitedStringToArray(String str, String delimiter) {
        str = str.trim();
        if (str.startsWith("[")) {
            str = str.substring(1);
        }
        if (str.endsWith("]")) {
            str = str.substring(0,str.length()-1);
        }
        String ret[] = str.split(delimiter);
        for (int i=0; i< ret.length; i++) {
            ret[i] = ret[i].trim();
        }
        return ret;
    }
}
