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

public class Payment extends Event {

    private final int amount;

    public Payment(int userId, long timestamp, int amount) {
        super(userId, timestamp);
        this.amount = amount;
    }

    public int amount() {
        return amount;
    }


    @Override
    public boolean equals(Object obj) {
        final Payment that;
        return obj instanceof Payment
                && this.userId() == (that = (Payment) obj).userId()
                && this.amount == that.amount;
    }

    @Override
    public int hashCode() {
        int hc = 17;
        hc = 73 * hc + userId();
        hc = 73 * hc + amount;
        return hc;
    }

}
