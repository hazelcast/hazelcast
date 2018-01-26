/*
 * Copyright (c) 2008-2017, Hazelcast, Inc. All Rights Reserved.
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

import com.hazelcast.nio.serialization.PortableReader;
import com.hazelcast.nio.serialization.PortableWriter;
import com.hazelcast.nio.serialization.BinaryInterface;
import com.hazelcast.spi.impl.SpiPortableHook;

import java.io.IOException;

import static com.hazelcast.util.Preconditions.checkNotNull;
import static com.hazelcast.util.StringUtil.bytesToString;
import static com.hazelcast.util.StringUtil.stringToBytes;

/**
 * Simple implementation of {@link Credentials} using
 * username and password as security attributes.
 */
@BinaryInterface
public class UsernamePasswordCredentials extends AbstractCredentials {

    private static final long serialVersionUID = -1508314631354255039L;

    private byte[] password;

    public UsernamePasswordCredentials() {
    }

    public UsernamePasswordCredentials(String username, String password) {
        super(username);
        checkNotNull(password);
        this.password = stringToBytes(password);
    }

    /**
     * Gets the user name.
     *
     * @return the user name
     */
    public String getUsername() {
        return getPrincipal();
    }

    /**
     * Gets the password.
     *
     * @return the password
     */
    public String getPassword() {
        checkNotNull(password);
        return bytesToString(password);
    }

    /**
     * Sets the user name.
     *
     * @param username the user name to set
     */
    public void setUsername(String username) {
        setPrincipal(username);
    }

    /**
     * Sets the password.
     *
     * @param password the password to set
     */
    public void setPassword(String password) {
        checkNotNull(password);
        this.password = stringToBytes(password);
    }

    @Override
    protected void writePortableInternal(PortableWriter writer) throws IOException {
        writer.writeByteArray("pwd", password);
    }

    @Override
    protected void readPortableInternal(PortableReader reader) throws IOException {
        password = reader.readByteArray("pwd");
    }

    public int getFactoryId() {
        return SpiPortableHook.ID;
    }

    @Override
    public int getClassId() {
        return SpiPortableHook.USERNAME_PWD_CRED;
    }

    @Override
    public String toString() {
        return "UsernamePasswordCredentials [username=" + getUsername() + "]";
    }
}
