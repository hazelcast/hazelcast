/*
 * Copyright (c) 2008-2012, Hazel Bilisim Ltd. All Rights Reserved.
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

package com.hazelcast.core;

/**
 * Hazelcast transaction interface.
 * <p/>
 * <b>Note that Hazelcast doesn't support two phase commit (XA) transactions. </b>
 *
 * @see Hazelcast#getTransaction()
 */
public interface Transaction {

    public static final int TXN_STATUS_NO_TXN = 0;
    public static final int TXN_STATUS_ACTIVE = 1;
    public static final int TXN_STATUS_PREPARED = 2;
    public static final int TXN_STATUS_COMMITTED = 3;
    public static final int TXN_STATUS_ROLLED_BACK = 4;
    public static final int TXN_STATUS_PREPARING = 5;
    public static final int TXN_STATUS_COMMITTING = 6;
    public static final int TXN_STATUS_ROLLING_BACK = 7;
    public static final int TXN_STATUS_UNKNOWN = 8;

    /**
     * Creates a new transaction and associate it with the current thread.
     *
     * @throws IllegalStateException if transaction is already began
     */
    void begin() throws IllegalStateException;

    /**
     * Commits the transaction associated with the current thread.
     *
     * @throws IllegalStateException if transaction didn't begin.
     */
    void commit() throws IllegalStateException;

    /**
     * Rolls back the transaction associated with the current thread.
     *
     * @throws IllegalStateException if transaction didn't begin.
     */
    void rollback() throws IllegalStateException;

    /**
     * Returns the status of the transaction associated with the current thread.
     *
     * @return the status
     */
    int getStatus();
}
