/*
 * Copyright (c) 2008-2020, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.jet.examples.cogroup.datamodel;

import com.hazelcast.jet.impl.util.Util;

public class AddToCart extends Event {

    private final int quantity;

    public AddToCart(long timestamp, int userId, int quantity) {
        super(timestamp, userId);
        this.quantity = quantity;
    }

    public int quantity() {
        return quantity;
    }

    @Override
    public boolean equals(Object obj) {
        final AddToCart that;
        return obj instanceof AddToCart
                && this.timestamp() == (that = (AddToCart) obj).timestamp()
                && this.userId() == that.userId()
                && this.quantity == that.quantity;
    }

    @Override
    public int hashCode() {
        int hc = 17;
        hc = 73 * hc + Long.hashCode(timestamp());
        hc = 73 * hc + userId();
        hc = 73 * hc + quantity;
        return hc;
    }

    @Override
    public String toString() {
        return "AddToCart{" +
                "quantity=" + quantity +
                ", userId=" + userId() +
                ", timestamp=" + Util.toLocalTime(timestamp()) +
                '}';
    }
}
