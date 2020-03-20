/*
 * Copyright (c) 2008-2018, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.xa;

import com.atomikos.icatch.jta.UserTransactionManager;
import com.hazelcast.config.Config;
import com.hazelcast.config.InMemoryFormat;
import com.hazelcast.config.MapConfig;
import com.hazelcast.config.NearCacheConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IMap;
import com.hazelcast.core.TransactionalMap;
import com.hazelcast.test.HazelcastSerialParametersRunnerFactory;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.QuickTest;
import com.hazelcast.transaction.HazelcastXAResource;
import com.hazelcast.transaction.TransactionContext;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import javax.transaction.Transaction;
import javax.transaction.xa.XAResource;
import java.io.File;
import java.io.FilenameFilter;
import java.util.Collection;

import static java.util.Arrays.asList;
import static org.junit.Assert.assertEquals;

@RunWith(Parameterized.class)
@Parameterized.UseParametersRunnerFactory(HazelcastSerialParametersRunnerFactory.class)
@Category(QuickTest.class)
public class NearCacheXATest extends HazelcastTestSupport {

    @Parameterized.Parameters(name = "format:{0} serializeKeys:{1}")
    public static Collection<Object[]> parameters() {
        return asList(new Object[][]{
                {InMemoryFormat.BINARY, true},
                {InMemoryFormat.BINARY, false},
                {InMemoryFormat.OBJECT, true},
                {InMemoryFormat.OBJECT, false},
        });
    }

    @Parameterized.Parameter
    public InMemoryFormat inMemoryFormat;

    @Parameterized.Parameter(value = 1)
    public boolean serializeKeys;


    private UserTransactionManager tm;

    public void cleanAtomikosLogs() {
        try {
            File currentDir = new File(".");
            final File[] tmLogs = currentDir.listFiles(new FilenameFilter() {
                @Override
                public boolean accept(File dir, String name) {
                    return name.endsWith(".epoch") || name.startsWith("tmlog");
                }
            });
            for (File tmLog : tmLogs) {
                tmLog.delete();
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Before
    public void setUp() throws Exception {
        cleanAtomikosLogs();
        tm = new UserTransactionManager();
        tm.setTransactionTimeout(60);
    }

    @After
    public void tearDown() throws Exception {
        tm.close();
        cleanAtomikosLogs();
    }

    @Test
    public void after_txn_commit_near_cache_should_be_invalidated() throws Exception {
        Config cfg = getConfig();
        String mapName = "cache";
        MapConfig cacheConfig = cfg.getMapConfig(mapName);
        NearCacheConfig nearCacheConfig = new NearCacheConfig();
        nearCacheConfig.setInvalidateOnChange(true)
                .setCacheLocalEntries(true)
                .setInMemoryFormat(inMemoryFormat)
                .setSerializeKeys(serializeKeys);

        cacheConfig.setNearCacheConfig(nearCacheConfig);

        HazelcastInstance server = createHazelcastInstance(cfg);
        IMap map = server.getMap(mapName);

        String key = "key";
        String oldValue = "oldValue";
        String updatedValue = "updatedValue";

        // populate imap
        map.put(key, oldValue);

        // populate near cache
        Object valueReadBeforeTxnFromNonTxnMap = map.get(key);

        HazelcastXAResource xaResource = server.getXAResource();

        // begin txn
        tm.begin();
        Object valueReadInsideTxnFromTxnMapBeforeUpdate = null;
        Object valueReadInsideTxnFromTxnMapAfterUpdate = null;
        Object valueReadInsideTxnFromNonTxnMapAfterUpdate = null;
        boolean error = false;
        try {
            Transaction transaction = tm.getTransaction();
            transaction.enlistResource(xaResource);
            TransactionContext ctx = xaResource.getTransactionContext();

            TransactionalMap txnMap = ctx.getMap(mapName);
            valueReadInsideTxnFromTxnMapBeforeUpdate = txnMap.get(key);

            txnMap.put(key, updatedValue);

            valueReadInsideTxnFromTxnMapAfterUpdate = txnMap.get(key);
            valueReadInsideTxnFromNonTxnMapAfterUpdate = map.get(key);
        } catch (Exception e) {
            e.printStackTrace();
            error = true;
        } finally {
            close(error, xaResource);
        }

        // check values read from txn map
        assertEquals(oldValue, valueReadInsideTxnFromTxnMapBeforeUpdate);
        assertEquals(updatedValue, valueReadInsideTxnFromTxnMapAfterUpdate);

        // check values read from non-txn map
        assertEquals(oldValue, valueReadBeforeTxnFromNonTxnMap);
        assertEquals(oldValue, valueReadInsideTxnFromNonTxnMapAfterUpdate);
        Object valueReadAfterTxnFromNonTxnMap = map.get(key);
        assertEquals(updatedValue, valueReadAfterTxnFromNonTxnMap);
    }

    private void close(boolean error, XAResource... xaResource) throws Exception {

        int flag = XAResource.TMSUCCESS;

        // get the current tx
        Transaction tx = tm.getTransaction();
        // closeConnection
        if (error) {
            flag = XAResource.TMFAIL;
        }
        for (XAResource resource : xaResource) {
            tx.delistResource(resource, flag);
        }

        if (error) {
            tm.rollback();
        } else {
            tm.commit();
        }
    }

}
