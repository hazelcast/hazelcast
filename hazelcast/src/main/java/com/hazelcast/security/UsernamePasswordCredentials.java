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

package com.hazelcast.security;

import static java.util.Objects.requireNonNull;

import java.io.IOException;

import com.hazelcast.nio.serialization.BinaryInterface;
import com.hazelcast.nio.serialization.Portable;
import com.hazelcast.nio.serialization.PortableReader;
import com.hazelcast.nio.serialization.PortableWriter;
import com.hazelcast.spi.impl.SpiPortableHook;

/**
 * Simple implementation of {@link PasswordCredentials} using
 * name and password as security attributes.
 */
@BinaryInterface
public class UsernamePasswordCredentials implements PasswordCredentials, Portable {

    private static final long serialVersionUID = -1508314631354255039L;

    private String name;
    private String password;

    public UsernamePasswordCredentials() {
    }

    public UsernamePasswordCredentials(String username, String password) {
        this.name = requireNonNull(username, "Username has to be provided.");
        this.password = requireNonNull(password, "Password has to be provided.");
    }

    /**
     * Gets the password.
     */
    @Override
    public String getPassword() {
        return password;
    }

    /**
     * @param password the password to set
     */
    public void setPassword(String password) {
        this.password = password;
    }

    @Override
    public String getName() {
        return name;
    }

    /**
     * @param name the name to set
     */
    public void setName(String name) {
        this.name = name;
    }

    @Override
    public int getFactoryId() {
        return SpiPortableHook.ID;
    }

    @Override
    public int getClassId() {
        return SpiPortableHook.USERNAME_PWD_CRED;
    }

    @Override
    public String toString() {
        return "UsernamePasswordCredentials [name=" + getName() + "]";
    }

    @Override
    public final void writePortable(PortableWriter writer) throws IOException {
        writer.writeUTF("name", name);
        writer.writeUTF("pwd", password);
        writePortableInternal(writer);
    }

    @Override
    public final void readPortable(PortableReader reader) throws IOException {
        name = reader.readUTF("name");
        password = reader.readUTF("pwd");
        readPortableInternal(reader);
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + ((name == null) ? 0 : name.hashCode());
        result = prime * result + ((password == null) ? 0 : password.hashCode());
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
        UsernamePasswordCredentials other = (UsernamePasswordCredentials) obj;
        if (name == null) {
            if (other.name != null) {
                return false;
            }
        } else if (!name.equals(other.name)) {
            return false;
        }
        if (password == null) {
            if (other.password != null) {
                return false;
            }
        } else if (!password.equals(other.password)) {
            return false;
        }
        return true;
    }

    protected void writePortableInternal(PortableWriter writer) throws IOException {
    }

    protected void readPortableInternal(PortableReader reader) throws IOException {
    }

}
