/*
 * Copyright (c) 2008-2012, Hazel Bilisim Ltd. All Rights Reserved.
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

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * Simple implementation of {@link Credentials} using
 * username and password as security attributes.
 */
public class UsernamePasswordCredentials extends AbstractCredentials {

    private static final long serialVersionUID = -1508314631354255039L;

    private byte[] password;

    public UsernamePasswordCredentials() {
        super();
    }

    public UsernamePasswordCredentials(String username, String password) {
        super(username);
        this.password = password.getBytes();
    }

    public String getUsername() {
        return getPrincipal();
    }

    public byte[] getRawPassword() {
        return password;
    }

    public String getPassword() {
        return new String(password);
    }

    public void setUsername(String username) {
        setPrincipal(username);
    }

    public void setPassword(String password) {
        this.password = password.getBytes();
    }

    public void writeDataInternal(DataOutput out) throws IOException {
        out.writeInt(password != null ? password.length : 0);
        if (password != null) {
            out.write(password);
        }
    }

    public void readDataInternal(DataInput in) throws IOException {
        int s = in.readInt();
        if (s > 0) {
            password = new byte[s];
            in.readFully(password);
        }
    }

    @Override
    public String toString() {
        return "UsernamePasswordCredentials [username=" + getUsername() + "]";
    }
}
