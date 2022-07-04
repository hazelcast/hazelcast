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

package com.hazelcast.gcp;

/**
 * Structure that represents the discovery output information.
 */
final class GcpAddress {
    private final String privateAddress;
    private final String publicAddress;

    GcpAddress(String privateAddress, String publicAddress) {
        this.privateAddress = privateAddress;
        this.publicAddress = publicAddress;
    }

    String getPrivateAddress() {
        return privateAddress;
    }

    String getPublicAddress() {
        return publicAddress;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        GcpAddress that = (GcpAddress) o;

        if (privateAddress != null ? !privateAddress.equals(that.privateAddress) : that.privateAddress != null) {
            return false;
        }
        return publicAddress != null ? publicAddress.equals(that.publicAddress) : that.publicAddress == null;
    }

    @Override
    public int hashCode() {
        int result = privateAddress != null ? privateAddress.hashCode() : 0;
        result = 31 * result + (publicAddress != null ? publicAddress.hashCode() : 0);
        return result;
    }

    @Override
    public String toString() {
        return "GcpAddress{"
                + "privateAddress='" + privateAddress + '\''
                + ", publicAddress='" + publicAddress + '\''
                + '}';
    }
}
