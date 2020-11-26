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

package com.hazelcast.jet.examples.files.unifiedapi;

import javax.annotation.Nonnull;
import java.io.Serializable;
import java.util.Objects;

/**
 * Information about a specific trade event that took place on a
 * financial exchange or any other kind of market.
 * <p>
 * A "trade event" is defined as the action by which one market
 * participant sells a certain amount (the "quantity") of a financial
 * instrument (the "ticker", most often a stock specified as the
 * short name of the company), to another market participant, at a
 * certain price.
 */
public class Trade implements Serializable {

    public long time;
    public String ticker;
    public long quantity;
    public long price;

    public Trade() {
    }

    public Trade(long time, @Nonnull String ticker, long quantity, long price) {
        this.time = time;
        this.ticker = Objects.requireNonNull(ticker);
        this.quantity = quantity;
        this.price = price;
    }

    /**
     * Event time of the trade.
     */
    public long getTime() {
        return time;
    }

    /**
     * Name of the instrument being traded.
     */
    @Nonnull
    public String getTicker() {
        return ticker;
    }

    /**
     * Quantity of the trade, the amount of the instrument that has been
     * traded.
     */
    public long getQuantity() {
        return quantity;
    }

    /**
     * Price at which the transaction took place.
     */
    public long getPrice() {
        return price;
    }

    @Override
    public String toString() {
        return "Trade{time=" + time + ", ticker='" + ticker + '\'' + ", quantity=" + quantity
                + ", price=" + price + '}';
    }
}
