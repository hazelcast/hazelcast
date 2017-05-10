package com.hazelcast.client.eviction;


import com.hazelcast.client.HazelcastClient;
import com.hazelcast.config.Config;
import com.hazelcast.config.MapConfig;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IMap;
import com.hazelcast.instance.GroupProperties;
import com.hazelcast.map.AbstractEntryProcessor;
import com.hazelcast.test.HazelcastTestSupport;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.Serializable;
import java.util.*;
import java.util.Map.Entry;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.junit.Assert.assertFalse;

public class MaxIdleTimeFailureTest extends HazelcastTestSupport {
    private static final String SESSIOND_MAP = "sessiond";
    private static AtomicBoolean failed = new AtomicBoolean(false);

    private SessionClient sc;
    private ExecutorService service = Executors.newCachedThreadPool();

    @Before
    public void setUp() {
        createServer();

        createClient();
    }

    @After
    public void tearDown() {
        service.shutdownNow();
        Hazelcast.shutdownAll();
        HazelcastClient.shutdownAll();
    }

    @Test
    public void testRemoveAfterVerify() throws Exception {

        for (int i = 0; i < 100; ++i) {
            service.submit(new Runnable() {
                @Override
                public void run() {
                    Random randy = new Random();
                    try {
                        while (true) {
                            Thread.sleep(randy.nextInt(100));
                            String sid = sc.create();
                            sc.verify(sid);
                            sc.setVar(sid, "foo", "bar");

                            Thread.sleep(13750 + randy.nextInt(500));

                            if (sc.verify(sid) != null) {
                                sc.removeVar(sid, "foo");
                            }
                        }
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                }
            });
        }

        Thread.sleep(60000);

        assertFalse(failed.get());
    }

    private class SessionClient {
        private HazelcastInstance instance;

        public SessionClient(HazelcastInstance instance) {
            this.instance = instance;
        }

        private IMap<String, Session> getMap() {
            return instance.getMap(SESSIOND_MAP);
        }

        public String create() {
            String sid = UUID.randomUUID().toString();
            return (String) getMap().executeOnKey(sid, new CreateSessionProcessor());
        }

        public Session verify(String sid) {
            return (Session) getMap().executeOnKey(sid, new VerifyProcessor());
        }

        public void setVar(String sid, String key, String value) {
            getMap().executeOnKey(sid, new SetVarEntryProcessor(key, value));
        }

        public void removeVar(String sid, String key) {
            getMap().executeOnKey(sid, new RemoveVarEntryProcessor(key));
        }

    }

    public static class CreateSessionProcessor extends AbstractEntryProcessor<String, Session> {
        private static final long serialVersionUID = 1L;

        @Override
        public Object process(Entry<String, Session> entry) {
            entry.setValue(new Session());
            return entry.getKey();
        }
    }

    public static class SetVarEntryProcessor extends AbstractEntryProcessor<String, Session> {
        private static final long serialVersionUID = 1L;
        private String[] mapping;

        public SetVarEntryProcessor(String key, String value) {
            mapping = new String[] { key, value };
        }

        @Override
        public Object process(Entry<String, Session> entry) {
            entry.getValue().vars.put(mapping[0], mapping[1]);
            entry.setValue(entry.getValue());
            return null;
        }

    }

    public static class RemoveVarEntryProcessor extends AbstractEntryProcessor<String, Session> {
        private static final long serialVersionUID = 1L;
        private String key;

        public RemoveVarEntryProcessor(String key) {
            this.key = key;
        }

        @Override
        public Object process(Entry<String, Session> entry) {
            if (entry.getValue() == null) {
                failed.set(true);
            }
            entry.getValue().vars.remove(key);
            entry.setValue(entry.getValue());
            return null;
        }
    }

    public static class VerifyProcessor extends AbstractEntryProcessor<String, Session> {
        private static final long serialVersionUID = 1L;

        @Override
        public Object process(Entry<String, Session> entry) {
            entry.setValue(entry.getValue());
            return entry.getValue();
        }
    }

    public static class Session implements Serializable {
        private static final long serialVersionUID = 1L;

        public Map<String, String> vars = new HashMap<String, String>();
    }

    private void createClient() {
        // Unfortunately this is only configurable via a system-wide property
        System.setProperty(GroupProperties.PROP_LOGGING_TYPE, "log4j");

        sc = new SessionClient(HazelcastClient.newHazelcastClient());
    }

    private void createServer() {
        Config config = new com.hazelcast.config.Config();

        MapConfig mapCfg = new MapConfig();
        mapCfg.setName(SESSIOND_MAP);
        mapCfg.setBackupCount(2);
        mapCfg.setTimeToLiveSeconds(86400);
        mapCfg.setMaxIdleSeconds(5);
        mapCfg.setReadBackupData(true);

        config.addMapConfig(mapCfg);

        createHazelcastInstanceFactory(2).newInstances(config);

    }
}
