package com.hazelcast.map.standalone;

import com.hazelcast.client.HazelcastClient;
import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.client.config.ClientNetworkConfig;
import com.hazelcast.config.*;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IMap;
import com.hazelcast.instance.GroupProperties;
import com.hazelcast.map.AbstractEntryProcessor;
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

public class IdleSessionEvictionTest {
    private static final String SESSIOND_MAP = "sessiond";
    private static AtomicBoolean failed = new AtomicBoolean(false);

    private List<String> addresses = Arrays.asList("localhost:5800", "localhost:5801");
    private HazelcastInstance server1, server2;
    private SessionClient sc;
    private ExecutorService service = Executors.newCachedThreadPool();

    @Before
    public void setUp() {
        server1 = createServer(5800);
        server2 = createServer(5801);
        sc = createClient();
    }

    @After
    public void tearDown() {
        service.shutdownNow();
        sc.shutdown();
        server1.shutdown();
        server2.shutdown();
    }

    @Test
    public void testRemoveAfterVerify() throws Exception {
        final List<String> sids = new ArrayList<String>();

        for (int i = 0; i < 200; ++i) {
            sids.add(sc.create());
        }


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
                        // Just shutting down
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

        public void shutdown() {
            instance.shutdown();
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

    private SessionClient createClient() {
        // Unfortunately this is only configurable via a system-wide property
        System.setProperty(GroupProperties.PROP_LOGGING_TYPE, "log4j");

        ClientConfig clientConfig = new ClientConfig();

        GroupConfig group = clientConfig.getGroupConfig();
        group.setName("r9.sessiond");
        group.setPassword("password");

        ClientNetworkConfig networkConfig = new ClientNetworkConfig();
        networkConfig.setAddresses(addresses);
        networkConfig.setRedoOperation(true);
        networkConfig.setConnectionAttemptLimit(Integer.MAX_VALUE);
        clientConfig.setNetworkConfig(networkConfig);

        return new SessionClient(HazelcastClient.newHazelcastClient(clientConfig));
    }

    private HazelcastInstance createServer(int port) {
        Config config = new com.hazelcast.config.Config("r9." + port);
        config.setProperty(GroupProperties.PROP_SOCKET_BIND_ANY, "false");
        config.setProperty(GroupProperties.PROP_LOGGING_TYPE, "log4j");
        config.setProperty(GroupProperties.PROP_ENABLE_JMX, "true");

        GroupConfig group = config.getGroupConfig();
        group.setName("r9.sessiond");
        group.setPassword("password");

        NetworkConfig network = config.getNetworkConfig();
        network.setPort(port);
        network.setPortAutoIncrement(false);

        JoinConfig join = network.getJoin();
        join.getMulticastConfig().setEnabled(false);
        join.getTcpIpConfig().setEnabled(true);
        join.getTcpIpConfig().setMembers(addresses);

        network.getInterfaces().setEnabled(false);

        MapConfig mapCfg = new MapConfig();
        mapCfg.setName(SESSIOND_MAP);
        mapCfg.setBackupCount(2);
        mapCfg.setTimeToLiveSeconds(86400);
        mapCfg.setMaxIdleSeconds(5);
        mapCfg.setReadBackupData(true);

        config.addMapConfig(mapCfg);

        HazelcastInstance hazelcastServer = Hazelcast.newHazelcastInstance(config);

        return hazelcastServer;

    }
}
