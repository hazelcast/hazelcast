/*
 * Copyright 2023 Hazelcast Inc.
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

package com.hazelcast.jet.sql.impl.connector.map.model;

import java.io.Serializable;
import java.util.Objects;

public class OrderKey implements Serializable {
    // FK, business partitioning key
    private String customerId;
    // PK, should be unique
    private long orderId;
    // attribute added for tests as a non-unique, non-partitioned attribute, not much practical use
    private String country;

    public OrderKey() {
    }

    public OrderKey(String customerId, long orderId, String country) {
        this.customerId = customerId;
        this.orderId = orderId;
        this.country = country;
    }

    public String getCustomerId() {
        return customerId;
    }

    public void setCustomerId(String customerId) {
        this.customerId = customerId;
    }

    public long getOrderId() {
        return orderId;
    }

    public void setOrderId(long orderId) {
        this.orderId = orderId;
    }

    public String getCountry() {
        return country;
    }

    public void setCountry(String country) {
        this.country = country;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        OrderKey orderKey = (OrderKey) o;
        return orderId == orderKey.orderId;
    }

    @Override
    public int hashCode() {
        return Objects.hash(orderId);
    }

    @Override
    public String toString() {
        return "OrderKey{" +
                "customerId='" + customerId + '\'' +
                ", orderId=" + orderId +
                ", country='" + country + '\'' +
                '}';
    }


}
