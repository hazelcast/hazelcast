/*
 * Copyright (c) 2008-2013, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.transaction;

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.DataSerializable;

import java.io.IOException;
import java.util.concurrent.TimeUnit;

/**
 * Contains the configuration for a Hazelcast transaction.
 */
public final class TransactionOptions implements DataSerializable {

    private long timeoutMillis;

    private int durability;

    private TransactionType transactionType;

    /**
     * Creates a new default configured TransactionsOptions.
     *
     * It will be configured with a timeout of 2 minutes, durability of 1 and a TransactionType.TWO_PHASE.
     */
    public TransactionOptions() {
        setTimeout(2, TimeUnit.MINUTES).setDurability(1).setTransactionType(TransactionType.TWO_PHASE);
    }

    /**
     * Gets the {@link TransactionType}.
     *
     * @return the TransactionType.
     */
    public TransactionType getTransactionType() {
        return transactionType;
    }

    /**
     * Sets the {@link TransactionType}.
     *
     * A local transaction is less safe than a two phase transaction; when a member fails during the commit
     * of a local transaction, it could be that some of the changes are committed, while others are not and this
     * can leave your system in an inconsistent state.
     *
     * @param transactionType the new TransactionType.
     * @return the updated TransactionOptions.
     * @see #getTransactionType()
     * @see #setDurability(int)
     */
    public TransactionOptions setTransactionType(TransactionType transactionType) {
        if(transactionType == null){
            throw new IllegalArgumentException("transactionType can't be null");
        }
        this.transactionType = transactionType;
        return this;
    }

    /**
     * Gets the timeout in milliseconds.
     *
     * @return the timeout in milliseconds.
     * @see #setTimeout(long, java.util.concurrent.TimeUnit)
     */
    public long getTimeoutMillis() {
        return timeoutMillis;
    }

    /**
     * Sets the timeout.
     *
     * The timeout determines the maximum lifespan of a transaction. So if a transaction is configured with a
     * timeout of 2 minutes, then it will automatically rollback if it hasn't committed yet.
     *
     * @param timeout  the timeout.
     * @param timeUnit the TimeUnit of the timeout.
     * @return the updated TransactionOptions
     * @throws IllegalArgumentException if timeout smaller or equal than 0, or timeUnit is null.
     * @see #getTimeoutMillis()
     */
    public TransactionOptions setTimeout(long timeout, TimeUnit timeUnit) {
        if (timeout <= 0) {
            throw new IllegalArgumentException("Timeout must be positive!");
        }
        if(timeUnit == null){
            throw new IllegalArgumentException("timeunit can't be null");
        }
        this.timeoutMillis = timeUnit.toMillis(timeout);
        return this;
    }

    /**
     * Gets the transaction durability.
     *
     * @return the transaction durability.
     * @see #setDurability(int)
     */
    public int getDurability() {
        return durability;
    }

    /**
     * Sets the transaction durability.
     *
     * The durability is the number of machines that can take over if a member fails during a transaction
     * commit or rollback. This value only has meaning when {@link TransactionType#TWO_PHASE} is selected.
     *
     * @param durability  the durability
     * @return the updated TransactionOptions.
     * @throws IllegalArgumentException if durability smaller than 0.
     */
    public TransactionOptions setDurability(int durability) {
        if (durability < 0) {
            throw new IllegalArgumentException("Durability cannot be negative!");
        }
        this.durability = durability;
        return this;
    }

    /**
     * Creates a new TransactionOptions configured with default settings.
     *
     * @return the created default TransactionOptions.
     * @see #TransactionOptions()
     */
    public static TransactionOptions getDefault() {
        return new TransactionOptions();
    }

    public void writeData(ObjectDataOutput out) throws IOException {
        out.writeLong(timeoutMillis);
        out.writeInt(durability);
        out.writeInt(transactionType.value);
    }

    public void readData(ObjectDataInput in) throws IOException {
        timeoutMillis = in.readLong();
        durability = in.readInt();
        transactionType = TransactionType.getByValue(in.readInt());
    }


    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder();
        sb.append("TransactionOptions");
        sb.append("{timeoutMillis=").append(timeoutMillis);
        sb.append(", durability=").append(durability);
        sb.append(", txType=").append(transactionType.value);
        sb.append('}');
        return sb.toString();
    }

    public enum TransactionType {
        TWO_PHASE(1), LOCAL(2);

        private final int value;

        TransactionType(int value){
            this.value = value;
        }

        public static TransactionType getByValue(int value){
            for (TransactionType type: values()){
                if (type.value == value){
                    return type;
                }
            }
            return TWO_PHASE;
        }
    }
}
