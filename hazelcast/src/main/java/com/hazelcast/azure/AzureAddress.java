/*
 * Copyright (c) 2008-2021, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.azure;

/**
 * Structure that represents the discovery output information. It is used to store public and private IP Addresses that
 * is returned from Azure API.
 */
final class AzureAddress {
    private final String privateAddress;
    private final String publicAddress;

    AzureAddress(String privateAddress, String publicAddress) {
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

        AzureAddress that = (AzureAddress) o;

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
        return "AzureAddress{"
                + "privateAddress='" + privateAddress + '\''
                + ", publicAddress='" + publicAddress + '\''
                + '}';
    }
}
