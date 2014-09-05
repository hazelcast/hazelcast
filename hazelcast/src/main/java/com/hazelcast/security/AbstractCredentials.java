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

package com.hazelcast.security;

import com.hazelcast.nio.serialization.Portable;
import com.hazelcast.nio.serialization.PortableReader;
import com.hazelcast.nio.serialization.PortableWriter;

import java.io.IOException;

/**
 * Abstract implementation of {@link Credentials}
 */
public abstract class AbstractCredentials implements Credentials, Portable {

    private static final long serialVersionUID = 3587995040707072446L;

    private String endpoint;
    private String principal;

    public AbstractCredentials() {
    }

    public AbstractCredentials(String principal) {
        this.principal = principal;
    }

    @Override
    public final String getEndpoint() {
        return endpoint;
    }

    @Override
    public final void setEndpoint(String endpoint) {
        this.endpoint = endpoint;
    }

    @Override
    public String getPrincipal() {
        return principal;
    }

    public void setPrincipal(String principal) {
        this.principal = principal;
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        if (principal == null) {
            result = prime * result;
        } else {
            result = prime * result + principal.hashCode();
        }
        return result;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null) {
            return false;
        }
        if (getClass() != obj.getClass()) {
            return false;
        }
        AbstractCredentials other = (AbstractCredentials) obj;
        if (principal == null) {
            if (other.principal != null) {
                return false;
            }
        } else if (!principal.equals(other.principal)) {
            return false;
        }
        return true;
    }

    @Override
    public final void writePortable(PortableWriter writer) throws IOException {
        writer.writeUTF("principal", principal);
        writer.writeUTF("endpoint", endpoint);
        writePortableInternal(writer);
    }

    @Override
    public final void readPortable(PortableReader reader) throws IOException {
        principal = reader.readUTF("principal");
        endpoint = reader.readUTF("endpoint");
        readPortableInternal(reader);
    }

    protected abstract void writePortableInternal(PortableWriter writer) throws IOException;

    protected abstract void readPortableInternal(PortableReader reader) throws IOException;
}
