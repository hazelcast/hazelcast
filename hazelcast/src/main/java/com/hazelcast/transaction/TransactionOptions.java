/*
 * Copyright (c) 2008-2022, Hazelcast, Inc. All Rights Reserved.
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
import com.hazelcast.internal.serialization.SerializableByConvention;

import java.io.IOException;
import java.util.concurrent.TimeUnit;

import static com.hazelcast.internal.serialization.SerializableByConvention.Reason.PUBLIC_API;

/**
 * Contains the configuration for a Hazelcast transaction.
 */
@SerializableByConvention(PUBLIC_API)
public final class TransactionOptions implements DataSerializable {

    /**
     * 2 minutes as default timeout value
     */
    public static final long DEFAULT_TIMEOUT_MILLIS = TimeUnit.MINUTES.toMillis(2);

    private long timeoutMillis;

    private int durability;

    private TransactionType transactionType;


    /**
     * Creates a new default configured TransactionsOptions.
     * <p>
     * It will be configured with a timeout of 2 minutes, durability of 1 and a TransactionType.TWO_PHASE.
     */
    public TransactionOptions() {
        setDurability(1).setTransactionType(TransactionType.TWO_PHASE).setDefaultTimeout();
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
     * <p>
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
        if (transactionType == null) {
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
     * <p>
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
        if (timeout < 0) {
            throw new IllegalArgumentException("Timeout can not be negative!");
        }
        if (timeUnit == null) {
            throw new IllegalArgumentException("timeunit can't be null");
        }
        if (timeout == 0) {
            setDefaultTimeout();
        } else {
            this.timeoutMillis = timeUnit.toMillis(timeout);
        }
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
     * <p>
     * The durability is the number of machines that can take over if a member fails during a transaction
     * commit or rollback. This value only has meaning when {@link TransactionType#TWO_PHASE} is selected.
     *
     * @param durability the durability
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

    private void setDefaultTimeout() {
        timeoutMillis = DEFAULT_TIMEOUT_MILLIS;
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        out.writeLong(timeoutMillis);
        out.writeInt(durability);
        out.writeInt(transactionType.id);
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        timeoutMillis = in.readLong();
        durability = in.readInt();
        transactionType = TransactionType.getById(in.readInt());
    }


    @Override
    public String toString() {
        return "TransactionOptions{"
                + "timeoutMillis=" + timeoutMillis
                + ", durability=" + durability
                + ", txType=" + transactionType
                + '}';
    }

    /**
     * The type of transaction. With the type you have
     * influence on how much guarantee you get when a
     * member crashes when a transaction is committing.
     */
    public enum TransactionType {

        /**
         * The one phase transaction executes a transaction using a single
         * step at the end; committing the changes. There is no prepare
         * of the transactions, so conflicts are not detected. If there
         * is a conflict, then when the transaction commits the changes,
         * some of the changes are written and others are not; leaving
         * the system in a potentially permanent inconsistent state.
         */
        ONE_PHASE(1),

        /**
         * The two phase commit is separated in 2 parts. First it tries to execute
         * the prepare; if there are any conflicts, the prepare will fail. Once the
         * prepare has succeeded, the commit (writing the changes) can be executed.
         *
         * Hazelcast also provides three phase transaction by
         * automatically copying the backlog to another member so
         * that in case of failure during a commit, another member
         * can continue the commit from backup. For more information
         * see the {@link TransactionOptions#setDurability(int)}
         */
        TWO_PHASE(2);

        private final int id;

        TransactionType(int id) {
            this.id = id;
        }

        public int id() {
            return id;
        }

        public static TransactionType getById(int id) {
            switch (id) {
                case 1:
                    return ONE_PHASE;
                case 2:
                    return TWO_PHASE;
                default:
                    throw new IllegalArgumentException("Unrecognized id:" + id);
            }
        }
    }
}
