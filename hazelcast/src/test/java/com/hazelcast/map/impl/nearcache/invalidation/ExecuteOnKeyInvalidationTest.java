/*
 * Copyright (c) 2008-2022, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.map.impl.nearcache.invalidation;

import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.client.test.TestHazelcastFactory;
import com.hazelcast.config.NearCacheConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.map.IMap;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.Collections;

import static org.junit.Assert.assertEquals;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class ExecuteOnKeyInvalidationTest extends HazelcastTestSupport {

  private final TestHazelcastFactory factory = new TestHazelcastFactory();
  private HazelcastInstance client;
  private HazelcastInstance serializedKeyClient;

  @Before
  public void setUp() {
    startHazelcastInstance();
    client = clientWithNearCache();
    serializedKeyClient = clientWithSerializedKeysNearCache();
  }

  @After
  public void tearDown() {
    factory.shutdownAll();
  }

  private HazelcastInstance clientWithNearCache() {
    ClientConfig clientConfig = new ClientConfig().addNearCacheConfig(new NearCacheConfig("map"));
    return factory.newHazelcastClient(clientConfig);
  }

  private HazelcastInstance clientWithSerializedKeysNearCache() {
    ClientConfig clientConfig = new ClientConfig().addNearCacheConfig(
        new NearCacheConfig("serializedKeyMap").setSerializeKeys(true));
    return factory.newHazelcastClient(clientConfig);
  }

  private void startHazelcastInstance() {
    factory.newHazelcastInstance();
  }

  @Test
  public void testExecuteOnKeysInvalidation_withSerializedKeys() {
    IMap<String, String> serializedKeyMap = serializedKeyClient.getMap("serializedKeyMap");
    serializedKeyMap.put("key", "old-value");
    serializedKeyMap.get("key");
    serializedKeyMap.executeOnKeys(
        Collections.singleton("key"),
        entry -> {
          entry.setValue("new-value");
          return null;
        }
    );
    assertEquals("new-value", serializedKeyMap.get("key"));
  }

  @Test
  public void testExecuteOnKeysInvalidation_withNullOperationResult() {
    IMap<String, String> map = client.getMap("map");
    map.put("key", "old-value");
    map.get("key");
    map.executeOnKeys(
        Collections.singleton("key"),
        entry -> {
          entry.setValue("new-value");
          return null;
        }
    );
    assertEquals("new-value", map.get("key"));
  }

  @Test
  public void testExecuteOnKeysInvalidation_withOperationResult() {
    IMap<String, String> map = client.getMap("map");
    map.put("key", "old-value");
    map.get("key");
    map.executeOnKeys(
        Collections.singleton("key"),
        entry -> {
          entry.setValue("new-value");
          return "computation-result-of-on-key-execution";
        }
    );
    assertEquals("new-value", map.get("key"));
  }
}
