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

package com.hazelcast.client.impl.client;

import com.hazelcast.nio.serialization.Portable;
import com.hazelcast.nio.serialization.PortableReader;
import com.hazelcast.nio.serialization.PortableWriter;

import java.io.IOException;

public final class ClientPrincipal implements Portable {

    private String uuid;
    private String ownerUuid;

    public ClientPrincipal() {
    }

    public ClientPrincipal(String uuid, String ownerUuid) {
        this.uuid = uuid;
        this.ownerUuid = ownerUuid;
    }

    public String getUuid() {
        return uuid;
    }

    public String getOwnerUuid() {
        return ownerUuid;
    }

    @Override
    public int getFactoryId() {
        return ClientPortableHook.ID;
    }

    @Override
    public int getClassId() {
        return ClientPortableHook.PRINCIPAL;
    }

    @Override
    public void writePortable(PortableWriter writer) throws IOException {
        writer.writeUTF("uuid", uuid);
        writer.writeUTF("ownerUuid", ownerUuid);
    }

    @Override
    public void readPortable(PortableReader reader) throws IOException {
        uuid = reader.readUTF("uuid");
        ownerUuid = reader.readUTF("ownerUuid");
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        ClientPrincipal that = (ClientPrincipal) o;

        if (ownerUuid != null ? !ownerUuid.equals(that.ownerUuid) : that.ownerUuid != null) {
            return false;
        }
        if (uuid != null ? !uuid.equals(that.uuid) : that.uuid != null) {
            return false;
        }

        return true;
    }

    @Override
    public int hashCode() {
        int result = uuid != null ? uuid.hashCode() : 0;
        result = 31 * result + (ownerUuid != null ? ownerUuid.hashCode() : 0);
        return result;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("ClientPrincipal{");
        sb.append("uuid='").append(uuid).append('\'');
        sb.append(", ownerUuid='").append(ownerUuid).append('\'');
        sb.append('}');
        return sb.toString();
    }
}
