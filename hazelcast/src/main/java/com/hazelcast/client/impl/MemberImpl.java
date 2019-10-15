/*
 * Copyright (c) 2008-2019, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.client.impl;

import com.hazelcast.cluster.Member;
import com.hazelcast.cluster.impl.AbstractMember;
import com.hazelcast.logging.ILogger;
import com.hazelcast.cluster.Address;
import com.hazelcast.nio.serialization.DataSerializable;
import com.hazelcast.internal.serialization.SerializableByConvention;
import com.hazelcast.version.MemberVersion;

import java.util.Map;
import java.util.UUID;

import static com.hazelcast.instance.EndpointQualifier.MEMBER;
import static com.hazelcast.internal.serialization.SerializableByConvention.Reason.INHERITANCE;
import static java.util.Collections.singletonMap;

/**
 * Client side specific Member implementation.
 *
 * <p>Caution: This class is required by protocol encoder/decoder which are on the Hazelcast module.
 * So this implementation also stays in the same module, although it is totally client side specific.</p>
 *
 * This class is marked as serializable by convention as it implements interface {@link Member} which
 * is {@link DataSerializable} and not {@code IdentifiedDataSerializable}. Actual serialization
 * in client protocol communication is performed by a dedicated {@code MemberCodec}.
 */
@SerializableByConvention(INHERITANCE)
public final class MemberImpl extends AbstractMember implements Member {

    public MemberImpl() {
    }

    public MemberImpl(Address address, MemberVersion version) {
        super(singletonMap(MEMBER, address), version, null, null, false);
    }

    public MemberImpl(Address address, MemberVersion version, UUID uuid) {
        super(singletonMap(MEMBER, address), version, uuid, null, false);
    }

    public MemberImpl(Address address, UUID uuid, Map<String, String> attributes, boolean liteMember) {
        super(singletonMap(MEMBER, address), MemberVersion.UNKNOWN, uuid, attributes, liteMember);
    }

    public MemberImpl(Address address, MemberVersion version, UUID uuid, Map<String, String> attributes, boolean liteMember) {
        super(singletonMap(MEMBER, address), version, uuid, attributes, liteMember);
    }

    public MemberImpl(AbstractMember member) {
        super(member);
    }

    @Override
    protected ILogger getLogger() {
        return null;
    }

    @Override
    public boolean localMember() {
        return false;
    }

    @Override
    public void setAttribute(String key, String value) {
        throw notSupportedOnClient();
    }

    @Override
    public void removeAttribute(String key) {
        throw notSupportedOnClient();
    }

    private UnsupportedOperationException notSupportedOnClient() {
        return new UnsupportedOperationException("Attributes on remote members must not be changed");
    }
}
