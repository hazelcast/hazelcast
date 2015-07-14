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
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;

@RunWith(HazelcastParallelClassRunner.class)
@Category(QuickTest.class)
public class ConnectionFactoryMetaDataTest {

    private ConnectionFactoryMetaData connectionFactoryMetaData = new ConnectionFactoryMetaData();

    @Test
    public void testConnectionFactoryMetaData() {
        assertNull(connectionFactoryMetaData.getAdapterName());
        assertNull(connectionFactoryMetaData.getAdapterShortDescription());
        assertNull(connectionFactoryMetaData.getAdapterVendorName());
        assertNull(connectionFactoryMetaData.getAdapterVersion());
        assertNull(connectionFactoryMetaData.getSpecVersion());
        assertEquals(new String[]{}, connectionFactoryMetaData.getInteractionSpecsSupported());
        assertFalse(connectionFactoryMetaData.supportsExecuteWithInputAndOutputRecord());
        assertFalse(connectionFactoryMetaData.supportsExecuteWithInputRecordOnly());
        assertFalse(connectionFactoryMetaData.supportsLocalTransactionDemarcation());
    }
}