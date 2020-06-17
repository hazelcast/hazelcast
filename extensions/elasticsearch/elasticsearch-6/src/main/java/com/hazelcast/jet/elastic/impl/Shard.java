/*
 * Copyright 2020 Hazelcast Inc.
 *
 * Licensed under the Hazelcast Community License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://hazelcast.com/hazelcast-community-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.hazelcast.jet.elastic.impl;

import javax.annotation.Nonnull;
import java.io.Serializable;

class Shard implements Serializable {

    private static final long serialVersionUID = 1L;

    private final String index;
    private final int shard;
    // Primary/replica
    private final Prirep prirep;
    private final int docs;
    private final String state;
    private final String ip;
    private final String httpAddress;
    private final String node;

    Shard(@Nonnull String index, int shard, @Nonnull Prirep prirep, int docs,
          @Nonnull String state, @Nonnull String ip, @Nonnull String httpAddress,
          @Nonnull String node) {

        this.index = index;
        this.shard = shard;
        this.prirep = prirep;
        this.docs = docs;
        this.state = state;
        this.ip = ip;
        this.httpAddress = httpAddress;
        this.node = node;
    }

    public String indexShard() {
        return index + "-" + shard;
    }

    public String getIndex() {
        return index;
    }

    public int getShard() {
        return shard;
    }

    public Prirep getPrirep() {
        return prirep;
    }

    public int getDocs() {
        return docs;
    }

    public String getState() {
        return state;
    }

    public String getIp() {
        return ip;
    }

    public String getHttpAddress() {
        return httpAddress;
    }

    public String getNode() {
        return node;
    }

    public enum Prirep {
        /**
         * Primary
         */
        p,

        /**
         * Replica
         */
        r;
    }

    @Override
    public String toString() {
        return "Shard{" +
                "index='" + index + '\'' +
                ", shard=" + shard +
                ", prirep=" + prirep +
                ", docs=" + docs +
                ", state='" + state + '\'' +
                ", ip='" + ip + '\'' +
                ", httpAddress='" + httpAddress + '\'' +
                ", node='" + node + '\'' +
                '}';
    }
}
