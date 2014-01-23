/*
 * Copyright (c) 2008-2013, Hazelcast, Inc. All Rights Reserved.
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

import com.hazelcast.instance.MemberImpl;
import com.hazelcast.map.operation.MapOperationType;
import com.hazelcast.nio.IOUtil;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.DataSerializable;

import java.io.IOException;

import static com.hazelcast.cluster.MemberAttributeChangedOperation.DELTA_MEMBER_PROPERTIES_OP_PUT;
import static com.hazelcast.cluster.MemberAttributeChangedOperation.DELTA_MEMBER_PROPERTIES_OP_REMOVE;

public class MemberAttributeEvent extends MembershipEvent implements DataSerializable {

    private MapOperationType operationType;
    private String key;
    private Object value;
    private Member member;

    public MemberAttributeEvent() {
        super(null, null, MEMBER_ATTRIBUTE_CHANGED, null);
    }

    public MemberAttributeEvent(Cluster cluster, MemberImpl member, MapOperationType operationType, String key, Object value) {
        super(cluster, member, MEMBER_ATTRIBUTE_CHANGED, null);
        this.member = member;
        this.operationType = operationType;
        this.key = key;
        this.value = value;
    }

    public MapOperationType getOperationType() {
        return operationType;
    }

    public String getKey() {
        return key;
    }

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
        switch (operationType) {
            case PUT:
                out.writeByte(DELTA_MEMBER_PROPERTIES_OP_PUT);
                IOUtil.writeAttributeValue(value, out);
                break;
            case REMOVE:
                out.writeByte(DELTA_MEMBER_PROPERTIES_OP_REMOVE);
                break;
        }
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        key = in.readUTF();
        member = new MemberImpl();
        member.readData(in);
        int operation = in.readByte();
        switch (operation)
        {
            case DELTA_MEMBER_PROPERTIES_OP_PUT:
                operationType = MapOperationType.PUT;
                value = IOUtil.readAttributeValue(in);
                break;
            case DELTA_MEMBER_PROPERTIES_OP_REMOVE:
                operationType = MapOperationType.REMOVE;
                break;
        }
        this.source = member;
    }

}