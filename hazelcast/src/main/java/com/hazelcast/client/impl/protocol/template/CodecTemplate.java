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

package com.hazelcast.client.impl.protocol.template;

import com.hazelcast.annotation.Codec;
import com.hazelcast.client.impl.client.DistributedObjectInfo;
import com.hazelcast.map.impl.SimpleEntryView;
import com.hazelcast.nio.Address;
import com.hazelcast.nio.serialization.Data;

import java.util.Map;

//@GenerateCodec
public interface CodecTemplate {

    @Codec(Address.class)
    void Address(String hostname, int port);

    @Codec(com.hazelcast.client.impl.MemberImpl.class)
    void Member(Address address, String uuid, Map<String, String> attributes);

    @Codec(SimpleEntryView.class)
    void SimpleEntryView(Data key, Data value, long cost, long creationTime, long expirationTime, long hits, long lastAccessTime,
                         long lastStoredTime, long lastUpdateTime, long version, long evictionCriteriaNumber, long ttl);

    @Codec(DistributedObjectInfo.class)
    void DistributedObjectInfo(String name, String serviceName);
}
