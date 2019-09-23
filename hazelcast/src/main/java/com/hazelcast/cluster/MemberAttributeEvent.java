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

package com.hazelcast.cluster;

import com.hazelcast.cluster.impl.MemberImpl;
import com.hazelcast.internal.nio.IOUtil;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.DataSerializable;
import com.hazelcast.internal.serialization.SerializableByConvention;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

import java.io.IOException;
import java.util.Set;

import static com.hazelcast.cluster.MemberAttributeOperationType.PUT;
import static com.hazelcast.internal.serialization.SerializableByConvention.Reason.PUBLIC_API;
import static java.util.Collections.EMPTY_SET;
import static java.util.Collections.unmodifiableSet;

/**
 * Event for member attribute changes.
 */
@SuppressFBWarnings("SE_BAD_FIELD")
@SerializableByConvention(PUBLIC_API)
public class MemberAttributeEvent extends MembershipEvent implements DataSerializable {

    private MemberAttributeOperationType operationType;
    private String key;
    private Object value;

    public MemberAttributeEvent() {
        super(null, null, MEMBER_ATTRIBUTE_CHANGED, EMPTY_SET);
    }

    public MemberAttributeEvent(Cluster cluster, Member member, Set<Member> members,
                                MemberAttributeOperationType operationType, String key, Object value) {
        super(cluster, member, MEMBER_ATTRIBUTE_CHANGED, members);
        this.member = member;
        this.operationType = operationType;
        this.key = key;
        this.value = value;
    }

    /**
     * Returns the type of member attribute change for this MemberAttributeEvent.
     *
     * @return the type of member attribute change for this MemberAttributeEvent
     */
    public MemberAttributeOperationType getOperationType() {
        return operationType;
    }

    /**
     * Returns the key for this MemberAttributeEvent.
     *
     * @return the key for this MemberAttributeEvent
     */
    public String getKey() {
        return key;
    }

    /**
     * Returns the value for this MemberAttributeEvent.
     *
     * @return the value for this MemberAttributeEvent
     */
    public Object getValue() {
        return value;
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        out.writeUTF(key);
        member.writeData(out);
        out.writeByte(operationType.getId());
        if (operationType == PUT) {
            IOUtil.writeAttributeValue(value, out);
        }
        out.writeObject(members);
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        key = in.readUTF();
        member = new MemberImpl();
        member.readData(in);
        operationType = MemberAttributeOperationType.getValue(in.readByte());
        if (operationType == PUT) {
            value = IOUtil.readAttributeValue(in);
        }
        this.source = member;
        this.members = unmodifiableSet(in.readObject());
    }
}
