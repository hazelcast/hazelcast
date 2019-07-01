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

public class Trade extends Event {
    private int productId;
    private int brokerId;
    private int marketId;

    Trade(int userId, long timestamp) {
        super(userId, timestamp);
    }

    public int productId() {
        return productId;
    }

    public int brokerId() {
        return brokerId;
    }

    public int marketId() {
        return marketId;
    }

    public String ticker() {
        return "ticker";
    }

    public long worth() { return 0; }

    public Trade setStockInfo(StockInfo info) {
        return this;
    }

    public Trade setProduct(Product product) {
        return this;
    }
}
