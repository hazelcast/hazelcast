/*
 * Copyright (c) 2008-2019, Hazelcast, Inc. All Rights Reserved.
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

package datamodel;

public class AddToCart extends Event {

    private final int quantity;

    public AddToCart(int userId, long timestamp, int quantity) {
        super(userId, timestamp);
        this.quantity = quantity;
    }

    public AddToCart(int userId) {
        this(userId, 0, 0);
    }

    public int quantity() {
        return quantity;
    }

    @Override
    public boolean equals(Object obj) {
        final AddToCart that;
        return obj instanceof AddToCart
                && this.userId() == (that = (AddToCart) obj).userId()
                && this.quantity == that.quantity;
    }

    @Override
    public int hashCode() {
        int hc = 17;
        hc = 73 * hc + userId();
        hc = 73 * hc + quantity;
        return hc;
    }
}
