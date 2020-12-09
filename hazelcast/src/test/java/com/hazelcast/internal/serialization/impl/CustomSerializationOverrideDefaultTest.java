/*
 * Copyright (c) 2008-2020, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.internal.serialization.impl;

import com.hazelcast.config.SerializationConfig;
import com.hazelcast.config.SerializerConfig;
import com.hazelcast.internal.serialization.Data;
import com.hazelcast.internal.serialization.SerializationService;
import com.hazelcast.internal.serialization.impl.defaultserializers.ConstantSerializers.IntegerSerializer;
import com.hazelcast.spi.properties.ClusterProperty;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static org.junit.Assert.assertEquals;

@RunWith(HazelcastSerialClassRunner.class)
@Category(QuickTest.class)
public class CustomSerializationOverrideDefaultTest {


  @Test(expected = IllegalArgumentException.class)
  public void testSerializerDefault() {
    testSerializer(false);
  }

  @Test
  public void testSerializerDefaultOverridden() {
    testSerializer(true);
  }

  @Test
  public void testSerializerDefaultOverridden_systemPropertyTrue() {
    ClusterProperty.SERIALIZATION_ALLOW_OVERRIDE_DEFAULT_SERIALIZERS.setSystemProperty("true");
    testSerializer(false);
  }

  @Test(expected = IllegalArgumentException.class)
  public void testSerializerDefaultOverridden_systemPropertyFalse() {
    ClusterProperty.SERIALIZATION_ALLOW_OVERRIDE_DEFAULT_SERIALIZERS.setSystemProperty("false");
    testSerializer(true);
  }

  private void testSerializer(final boolean allowOverrideDefaultSerializers) {
    final SerializationConfig config = new SerializationConfig().setAllowOverrideDefaultSerializers(allowOverrideDefaultSerializers);
    final SerializerConfig sc = new SerializerConfig()
      .setImplementation(new IntegerSerializer())
      .setTypeClass(Integer.class);
    config.addSerializerConfig(sc);

    final SerializationService ss = new DefaultSerializationServiceBuilder().setConfig(config).build();

    final Integer answer = 42;
    final Data d = ss.toData(answer);
    final Integer deserializedAnswer = ss.toObject(d);

    assertEquals(answer, deserializedAnswer);
  }
}
