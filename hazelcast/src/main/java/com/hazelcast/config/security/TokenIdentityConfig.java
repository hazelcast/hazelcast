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

package com.hazelcast.config.security;

import static java.util.Objects.requireNonNull;

import java.util.Arrays;

import com.hazelcast.security.ICredentialsFactory;
import com.hazelcast.security.SimpleTokenCredentials;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

/**
 * Simple token identity configuration.
 */
@SuppressFBWarnings({"EI_EXPOSE_REP", "EI_EXPOSE_REP2"})
public class TokenIdentityConfig implements IdentityConfig {

    private final byte[] token;
    private final TokenEncoding encoding;

    private TokenIdentityConfig(TokenIdentityConfig tokenIdentityConfig) {
        this.token = tokenIdentityConfig.token;
        this.encoding = tokenIdentityConfig.encoding;
    }

    public TokenIdentityConfig(TokenEncoding encoding, String encodedToken) {
        this.encoding = requireNonNull(encoding);
        this.token = encoding.decode(requireNonNull(encodedToken));
    }

    public TokenIdentityConfig(byte[] token) {
        this.token = requireNonNull(token);
        this.encoding = TokenEncoding.getEncodingForBytes(token);
    }

    public String getTokenEncoded() {
        return encoding.encode(token);
    }

    public byte[] getToken() {
        return token;
    }

    public TokenEncoding getEncoding() {
        return encoding;
    }

    @Override
    public ICredentialsFactory asCredentialsFactory(ClassLoader cl) {
        return new StaticCredentialsFactory(new SimpleTokenCredentials(token));
    }

    @Override
    public IdentityConfig copy() {
        return new TokenIdentityConfig(this);
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + ((encoding == null) ? 0 : encoding.hashCode());
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
        TokenIdentityConfig other = (TokenIdentityConfig) obj;
        if (encoding != other.encoding) {
            return false;
        }
        if (!Arrays.equals(token, other.token)) {
            return false;
        }
        return true;
    }

    @Override
    public String toString() {
        StringBuilder builder = new StringBuilder();
        builder.append("TokenIdentityConfig [token=***, encoding=").append(encoding).append("]");
        return builder.toString();
    }

}
