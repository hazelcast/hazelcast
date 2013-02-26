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

package com.hazelcast.core;

import com.hazelcast.transaction.TransactionException;

public interface TransactionContext {

    Transaction beginTransaction();

    /**
     * Returns the transaction instance associated with the current context,
     * creates a new one if it wasn't already.
     * <p/>
     * Transaction doesn't start until you call <tt>transaction.begin()</tt> and
     * if a transaction is started then all transactional Hazelcast operations
     * are automatically transactional.
     * <pre>
     *  Map map = Hazelcast.getMap("mymap");
     *  Transaction txn = Hazelcast.getTransaction();
     *  txn.begin();
     *  try {
     *    map.put ("key", "value");
     *    txn.commit();
     *  }catch (Exception e) {
     *    txn.rollback();
     *  }
     * </pre>
     * Isolation is always <tt>REPEATABLE_READ</tt> . If you are in
     * a transaction, you can read the data in your transaction and the data that
     * is already committed and if not in a transaction, you can only read the
     * committed data. Implementation is different for queue and map/set. For
     * queue operations (offer,poll), offered and/or polled objects are copied to
     * the next member in order to safely commit/rollback. For map/set, Hazelcast
     * first acquires the locks for the write operations (put, remove) and holds
     * the differences (what is added/removed/updated) locally for each transaction.
     * When transaction is set to commit, Hazelcast will release the locks and
     * apply the differences. When rolling back, Hazelcast will simply releases
     * the locks and discard the differences. Transaction instance is attached
     * to the current thread and each Hazelcast operation checks if the current
     * thread holds a transaction, if so, operation will be transaction aware.
     * When transaction is committed, rolled back or timed out, it will be detached
     * from the thread holding it.
     *
     * @return transaction for the current context
     */
    Transaction getTransaction();

    void commitTransaction() throws TransactionException;

    void rollbackTransaction();

    /**
     * Returns the transactional distributed map instance with the specified name.
     *
     *
     * @param name name of the distributed map
     * @return transactional distributed map instance with the specified name
     */
    <K, V> DistributedObject getMap(String name);


    <T extends DistributedObject> T getDistributedObject(String serviceName, Object id);

}
