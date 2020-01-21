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

package com.hazelcast.jet.examples.faulttolerance;

import java.io.Serializable;

/**
 * Represents a single price update event
 */
public class PriceUpdateEvent implements Serializable {
    private final String ticker;
    private final int price;
    private final long timestamp;

    public PriceUpdateEvent(String ticker, Integer price, Long timestamp) {
        this.ticker = ticker;
        this.price = price;
        this.timestamp = timestamp;
    }

    public String ticker() {
        return ticker;
    }

    public int price() {
        return price;
    }

    public long timestamp() {
        return timestamp;
    }

    @Override
    public String toString() {
        return "{" + ticker + ", " + timestamp + ", " + price + "}";
    }
}
