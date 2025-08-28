/*
 * Copyright (c) 2008-2025, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.client.impl.protocol.util;

import com.hazelcast.client.impl.protocol.codec.MapPutCodec;
import com.hazelcast.client.impl.protocol.codec.builtin.DataCodec;
import com.hazelcast.internal.serialization.impl.HeapData;
import com.hazelcast.internal.serialization.impl.SerializationUtil;
import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import static org.assertj.core.api.Assertions.assertThat;

@Ignore("The test must be run manually or in isolation because it depends on static field in DataCodec")
public class ClientMessageEncoderDecoderKeepNonCanonicalNullTest extends ClientMessageEncoderDecoderTest {
    @Before
    public void setup() {
        System.setProperty(DataCodec.PROPERTY_KEEP_NON_CANONICAL_NULL, "true");
    }

    @After
    public void teardown() {
        System.clearProperty(DataCodec.PROPERTY_KEEP_NON_CANONICAL_NULL);
    }

    @Override
    @Test
    public void testPutNullKey() {
        var encoded = MapPutCodec.encodeRequest("map", new HeapData(new byte[100]), getNonNullData(), 5, 10);
        var decoded = MapPutCodec.decodeRequest(encoded);
        assertThat(decoded.key).isNotNull();
        assertThat(SerializationUtil.isNullData(decoded.key)).isTrue();
        assertThat(decoded.key.dataSize()).isEqualTo(92);
    }

    @Override
    @Test
    public void testPutNullValue() {
        var encoded = MapPutCodec.encodeRequest("map", getNonNullData(), new HeapData(new byte[100]), 5, 10);
        var decoded = MapPutCodec.decodeRequest(encoded);
        assertThat(decoded.value).isNotNull();
        assertThat(SerializationUtil.isNullData(decoded.value)).isTrue();
        assertThat(decoded.value.dataSize()).isEqualTo(92);

    }
}
