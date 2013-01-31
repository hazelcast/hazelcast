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

package com.hazelcast.config;

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.DataSerializable;
import com.hazelcast.util.ByteUtil;

import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

public class InterfacesConfig implements DataSerializable {

    private boolean enabled = false;

    private final Set<String> interfaceSet = new HashSet<String>();

    /**
     * @return the enabled
     */
    public boolean isEnabled() {
        return enabled;
    }

    /**
     * @param enabled the enabled to set
     */
    public InterfacesConfig setEnabled(final boolean enabled) {
        this.enabled = enabled;
        return this;
    }

    /**
     * Adds a new interface
     *
     * @param ip
     */
    public InterfacesConfig addInterface(final String ip) {
        interfaceSet.add(ip);
        return this;
    }

    /**
     * clears all interfaces.
     */
    public InterfacesConfig clear() {
        interfaceSet.clear();
        return this;
    }

    /**
     * @return a read-only collection of interfaces
     */
    public Collection<String> getInterfaces() {
        return Collections.unmodifiableCollection(interfaceSet);
    }

    /**
     * Adds a collection of interfaces.
     * Clears the current collection and then adds all entries of new collection.
     *
     * @param interfaces the interfaces to set
     */
    public InterfacesConfig setInterfaces(final Collection<String> interfaces) {
        clear();
        this.interfaceSet.addAll(interfaces);
        return this;
    }

    public void writeData(ObjectDataOutput out) throws IOException {
        boolean hasInterfaceSet = interfaceSet != null && !interfaceSet.isEmpty();
        out.writeByte(ByteUtil.toByte(enabled, hasInterfaceSet));
        if (hasInterfaceSet) {
            out.writeInt(interfaceSet.size());
            for (final String iface : interfaceSet) {
                out.writeUTF(iface);
            }
        }
    }

    public void readData(ObjectDataInput in) throws IOException {
        boolean b[] = ByteUtil.fromByte(in.readByte());
        enabled = b[0];
        boolean hasInterfaceSet = b[1];
        if (hasInterfaceSet) {
            interfaceSet.clear();
            int size = in.readInt();
            for (int i = 0; i < size; i++) {
                interfaceSet.add(in.readUTF());
            }
        }
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder();
        sb.append("InterfacesConfig");
        sb.append("{enabled=").append(enabled);
        sb.append(", interfaces=").append(interfaceSet);
        sb.append('}');
        return sb.toString();
    }
}
