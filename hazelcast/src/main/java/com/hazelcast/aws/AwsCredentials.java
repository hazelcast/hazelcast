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

package com.hazelcast.aws;

import java.util.Objects;

final class AwsCredentials {
    private String accessKey;
    private String secretKey;
    private String token;

    private AwsCredentials(String accessKey, String secretKey, String token) {
        this.accessKey = accessKey;
        this.secretKey = secretKey;
        this.token = token;
    }

    String getAccessKey() {
        return accessKey;
    }

    String getSecretKey() {
        return secretKey;
    }

    String getToken() {
        return token;
    }

    static Builder builder() {
        return new Builder();
    }

    static class Builder {
        private String accessKey;
        private String secretKey;
        private String token;

        Builder setAccessKey(String accessKey) {
            this.accessKey = accessKey;
            return this;
        }

        Builder setSecretKey(String secretKey) {
            this.secretKey = secretKey;
            return this;
        }

        Builder setToken(String token) {
            this.token = token;
            return this;
        }

        AwsCredentials build() {
            return new AwsCredentials(accessKey, secretKey, token);
        }
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        AwsCredentials that = (AwsCredentials) o;
        return Objects.equals(accessKey, that.accessKey)
            && Objects.equals(secretKey, that.secretKey)
            && Objects.equals(token, that.token);
    }

    @Override
    public int hashCode() {
        return Objects.hash(accessKey, secretKey, token);
    }
}
