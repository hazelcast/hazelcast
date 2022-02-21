/*
 * Copyright (c) 2008-2022, Hazelcast, Inc. All Rights Reserved.
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

import com.hazelcast.internal.serialization.BinaryInterface;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;
import com.hazelcast.spi.impl.SpiDataSerializerHook;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

import java.io.IOException;
import java.util.Arrays;

import static java.util.Objects.requireNonNull;

/**
 * Simple implementation of {@link Credentials} using a raw byte array token.
 */
@BinaryInterface
@SuppressFBWarnings("EI_EXPOSE_REP2")
public class SimpleTokenCredentials implements TokenCredentials, IdentifiedDataSerializable {

    private static final long serialVersionUID = -1508314631354255039L;

    private byte[] token;

    public SimpleTokenCredentials() {
    }

    public SimpleTokenCredentials(byte[] token) {
        requireNonNull(token, "Token has to be provided.");
        this.token = token;
    }

    /**
     * Gets the token.
     */
    @Override
    public byte[] getToken() {
        return token != null ? Arrays.copyOf(token, token.length) : null;
    }

    /**
     * Simple implementation which returns {@code "<empty>"} for {@code null} tokens and {@code "<token>"} for all other tokens.
     */
    @Override
    public String getName() {
        return token == null ? "<empty>" : "<token>";
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        out.writeByteArray(token);
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        token = in.readByteArray();
    }

    @Override
    public int getFactoryId() {
        return SpiDataSerializerHook.F_ID;
    }

    @Override
    public int getClassId() {
        return SpiDataSerializerHook.SIMPLE_TOKEN_CRED;
    }

    @Override
    public String toString() {
        return "SimpleTokenCredentials [tokenLength=" + (token != null ? token.length : 0) + "]";
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + Arrays.hashCode(token);
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
        SimpleTokenCredentials other = (SimpleTokenCredentials) obj;
        if (!Arrays.equals(token, other.token)) {
            return false;
        }
        return true;
    }

}
