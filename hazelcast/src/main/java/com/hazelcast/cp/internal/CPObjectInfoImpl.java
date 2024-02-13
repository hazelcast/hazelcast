/*
 * Copyright (c) 2008-2023, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.cp.internal;

import com.hazelcast.cp.CPGroupId;
import com.hazelcast.cp.CPObjectInfo;

import java.util.Objects;

public class CPObjectInfoImpl implements CPObjectInfo {

    private final String name;
    private final String serviceName;
    private final CPGroupId groupId;

    public CPObjectInfoImpl(String name, String serviceName, CPGroupId groupId) {
        this.name = name;
        this.serviceName = serviceName;
        this.groupId = groupId;
    }

    public String name() {
        return name;
    }

    public String serviceName() {
        return serviceName;
    }

    public CPGroupId groupId() {
        return groupId;
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == this) {
            return true;
        }
        if (obj == null || obj.getClass() != this.getClass()) {
            return false;
        }
        CPObjectInfoImpl that = (CPObjectInfoImpl) obj;
        return Objects.equals(this.name, that.name)
                && Objects.equals(this.serviceName, that.serviceName)
                && Objects.equals(this.groupId, that.groupId);
    }

    @Override
    public int hashCode() {
        return Objects.hash(name, serviceName, groupId);
    }

    @Override
    public String toString() {
        return "CPObjectInfoImpl["
                + "name=" + name + ", "
                + "serviceName=" + serviceName + ", "
                + "groupId=" + groupId + ']';
    }
}
