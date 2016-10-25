/*
 * Copyright (c) 2008-2016, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.cache.impl;

import com.hazelcast.core.Member;
import com.hazelcast.instance.MemberImpl;
import com.hazelcast.internal.serialization.impl.DefaultSerializationServiceBuilder;
import com.hazelcast.nio.Address;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.spi.serialization.SerializationService;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.net.UnknownHostException;

import static org.junit.Assert.assertEquals;

/**
 *
 */
@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelTest.class})
public class CachePartitionEventDataTest extends HazelcastTestSupport {

    private SerializationService serializationService;
    private CachePartitionEventData cachePartitionEventData;

    @Before
    public void setUp() throws UnknownHostException {
        serializationService = new DefaultSerializationServiceBuilder().build();
        Address address = new Address("127.0.0.1", 5701);
        Member member = new MemberImpl(address, true, false);
        cachePartitionEventData = new CachePartitionEventData("test", 1, member);
    }

    @Test
    public void testSerialization() {
        Data serializedEvent = serializationService.toData(cachePartitionEventData);
        CachePartitionEventData deserialized = serializationService.toObject(serializedEvent);
        assertEquals(cachePartitionEventData, deserialized);
    }

}