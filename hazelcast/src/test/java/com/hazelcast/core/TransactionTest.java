package com.hazelcast.core;

import org.junit.After;
import static org.junit.Assert.assertEquals;
import org.junit.Ignore;
import org.junit.Test;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.util.List;
import java.util.Iterator;
import java.util.concurrent.*;

public class TransactionTest {
    @Test
    @Ignore
    public void testMapPutSimple() {
        TransactionalMap txnMap = newTransactionalMapProxy("testMap");
        txnMap.begin();
        txnMap.put("1", "value");
        txnMap.commit();
    }


    @Test
    public void testMapPutCommitSize() {
        TransactionalMap txnMap = newTransactionalMapProxy("testMap");
        IMap imap = newMapProxy("testMap");
        txnMap.put("1", "item");
        assertEquals(1, txnMap.size());
        assertEquals(1, imap.size());
        txnMap.begin();
        txnMap.put(2, "newone");
        assertEquals(2, txnMap.size());
        assertEquals(1, imap.size());
        txnMap.commit();
        assertEquals(2, txnMap.size());
        assertEquals(2, imap.size());
    }
    @Test
    public void testMapPutRollbackSize() {
        TransactionalMap txnMap = newTransactionalMapProxy("testMap");
        IMap imap = newMapProxy("testMap");
        txnMap.put("1", "item");
        assertEquals(1, txnMap.size());
        assertEquals(1, imap.size());
        txnMap.begin();
        txnMap.put(2, "newone");
        assertEquals(2, txnMap.size());
        assertEquals(1, imap.size());
        txnMap.rollback();
        assertEquals(1, txnMap.size());
        assertEquals(1, imap.size());
    }

    @Test
    public void testMapPutWithTwoTxn() {
        TransactionalMap txnMap = newTransactionalMapProxy("testMap");
        TransactionalMap txnMap2 = newTransactionalMapProxy("testMap");
        txnMap.begin();
        txnMap.put("1", "value");
        txnMap.commit();
        txnMap2.begin();
        txnMap2.put("1", "value2");
        txnMap2.commit();
    }

    @Test
    public void testMapRemoveWithTwoTxn() {
        Hazelcast.getMap("testMap").put("1", "value");
        TransactionalMap txnMap = newTransactionalMapProxy("testMap");
        TransactionalMap txnMap2 = newTransactionalMapProxy("testMap");
        txnMap.begin();
        txnMap.remove("1");
        txnMap.commit();
        txnMap2.begin();
        txnMap2.remove("1");
        txnMap2.commit();
    }

    @Test
    public void testMapRemoveRollback() {
        Hazelcast.getMap("testMap").put("1", "value");
        TransactionalMap txnMap = newTransactionalMapProxy("testMap");
        TransactionalMap txnMap2 = newTransactionalMapProxy("testMap");
        txnMap.begin();
        assertEquals(1, txnMap.size());
        txnMap.remove("1");
        assertEquals(0, txnMap.size());
        assertEquals(1, txnMap2.size());
        txnMap.rollback();
        assertEquals(1, txnMap.size());
        assertEquals(1, txnMap2.size());
        txnMap2.begin();
        txnMap2.remove("1");
        txnMap2.commit();
        assertEquals(0, txnMap.size());
        assertEquals(0, txnMap2.size());
    }

    @Test
    public void testMapRemoveWithTwoTxn2() {
        TransactionalMap txnMap = newTransactionalMapProxy("testMap");
        TransactionalMap txnMap2 = newTransactionalMapProxy("testMap");
        txnMap.begin();
        txnMap.remove("1");
        txnMap.commit();
        txnMap2.begin();
        txnMap2.remove("1");
        txnMap2.commit();
    }

    @After
    public void cleanUp() {
        Iterator<IMap> it = mapsUsed.iterator();
        while (it.hasNext()) {
            IMap imap = it.next();
            imap.destroy();
        }
        mapsUsed.clear();
    }

    List<IMap> mapsUsed = new CopyOnWriteArrayList<IMap>();

    TransactionalMap newTransactionalMapProxy(String name) {
        IMap imap = Hazelcast.getMap(name);
        ClassLoader classLoader = Thread.currentThread().getContextClassLoader();
        Class[] interfaces = new Class[]{TransactionalMap.class};
        Object proxy = Proxy.newProxyInstance(classLoader, interfaces, new ThreadBoundInvocationHandler(imap));
        TransactionalMap txnalMap = (TransactionalMap) proxy;
        mapsUsed.add(txnalMap);
        return txnalMap;
    }

    IMap newMapProxy(String name) {
        IMap imap = Hazelcast.getMap(name);
        ClassLoader classLoader = Thread.currentThread().getContextClassLoader();
        Class[] interfaces = new Class[]{IMap.class};
        IMap proxy = (IMap) Proxy.newProxyInstance(classLoader, interfaces, new ThreadBoundInvocationHandler(imap));
        mapsUsed.add(proxy);
        return proxy;
    }

    interface TransactionalMap extends IMap {
        void begin();

        void commit();

        void rollback();
    }

    public static class ThreadBoundInvocationHandler implements InvocationHandler {
        final Object target;
        final ExecutorService es = Executors.newSingleThreadExecutor();
        final static Object NULL_OBJECT = new Object();

        public ThreadBoundInvocationHandler(Object target) {
            this.target = target;
        }

        public Object invoke(final Object o, final Method method, final Object[] objects) throws Throwable {
            final String name = method.getName();
            final BlockingQueue resultQ = new ArrayBlockingQueue(1);
            if (name.equals("begin") || name.equals("commit") || name.equals("rollback")) {
                es.execute(new Runnable() {
                    public void run() {
                        try {
                            Transaction txn = Hazelcast.getTransaction();
                            if (name.equals("begin")) {
                                txn.begin();
                            } else if (name.equals("commit")) {
                                txn.commit();
                            } else if (name.equals("rollback")) {
                                txn.rollback();
                            }
                            resultQ.put(NULL_OBJECT);
                        } catch (Exception e) {
                            try {
                                resultQ.put(e);
                            } catch (InterruptedException ignored) {
                            }
                        }
                    }
                });
            } else {
                es.execute(new Runnable() {
                    public void run() {
                        try {
                            Object result = method.invoke(target, objects);
                            resultQ.put((result == null) ? NULL_OBJECT : result);
                        } catch (Exception e) {
                            try {
                                resultQ.put(e);
                            } catch (InterruptedException ignored) {
                            }
                        }

                    }
                });
            }

            Object result = resultQ.take();
            if (name.equals("destroy")) {
                es.shutdown();
            }
            if (result instanceof Throwable) {
                throw ((Throwable) result);
            }
            return (result == NULL_OBJECT) ? null : result;

        }
    }


}
