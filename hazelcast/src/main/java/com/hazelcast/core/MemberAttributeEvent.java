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

package com.hazelcast.core;

import com.hazelcast.cluster.MemberAttributeOperationType;
import com.hazelcast.instance.MemberImpl;
import com.hazelcast.nio.IOUtil;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.DataSerializable;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

import java.io.IOException;

import static com.hazelcast.cluster.MemberAttributeOperationType.PUT;
import static java.util.Collections.EMPTY_SET;

@SuppressFBWarnings("SE_BAD_FIELD")
public class MemberAttributeEvent extends MembershipEvent implements DataSerializable {

    private MemberAttributeOperationType operationType;
    private String key;
    private Object value;
    private Member member;

    public MemberAttributeEvent() {
        super(null, null, MEMBER_ATTRIBUTE_CHANGED, EMPTY_SET);
    }

    public MemberAttributeEvent(Cluster cluster, Member member, MemberAttributeOperationType operationType,
                                String key, Object value) {
        super(cluster, member, MEMBER_ATTRIBUTE_CHANGED, EMPTY_SET);
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

    public Member getMember() {
        return member;
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        out.writeUTF(key);
        member.writeData(out);
        out.writeByte(operationType.getId());
        if (operationType == PUT) {
            IOUtil.writeAttributeValue(value, out);
        }
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
    }

}
