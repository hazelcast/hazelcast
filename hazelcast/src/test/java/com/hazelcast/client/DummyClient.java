package com.hazelcast.client;

import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.client.impl.clientside.HazelcastClientInstanceImpl;
import com.hazelcast.client.impl.clientside.HazelcastClientProxy;
import com.hazelcast.client.impl.connection.ClientConnection;
import com.hazelcast.client.impl.management.ClientConnectionProcessListener;
import com.hazelcast.cluster.Address;
import com.hazelcast.cluster.MembershipEvent;
import com.hazelcast.cluster.MembershipListener;
import com.hazelcast.config.Config;
import com.hazelcast.config.ListenerConfig;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.instance.impl.HazelcastInstanceFactory;
import com.hazelcast.internal.nio.Connection;
import com.hazelcast.internal.nio.ConnectionListener;
import org.jetbrains.annotations.NotNull;
import org.junit.After;
import org.junit.Test;

import static java.util.Arrays.asList;

public class DummyClient {

    @After
    public void after() {
        HazelcastInstanceFactory.terminateAll();
    }

    /**
     * Logs: (infinite loop):
     *
     * May 10, 2022 9:43:19 AM com.hazelcast.client.impl.connection.ClientConnectionManager
     * WARNING: hz.client_1 [dev] [5.1-SNAPSHOT] Unable to get live cluster connection, retry in 22498 ms, attempt: 65, cluster connect timeout: INFINITE, max backoff: 30000 ms
     */
    @Test
    public void addressNotFound() {
        ClientConfig config = newClientConfig();
        config.getNetworkConfig().addAddress("example.asdasd");
        config.getNetworkConfig().setConnectionTimeout(5000);
        HazelcastClient.newHazelcastClient(config);
    }

    /**
     * Logs: (in infinite loop):
     *
     * May 10, 2022 9:31:10 AM com.hazelcast.client.impl.connection.ClientConnectionManager
     * INFO: hz.client_1 [dev] [5.1-SNAPSHOT] Trying to connect to [erosb.eu]:5703
     * May 10, 2022 9:31:10 AM com.hazelcast.client.impl.connection.ClientConnectionManager
     * WARNING: hz.client_1 [dev] [5.1-SNAPSHOT] Exception during initial connection to [erosb.eu]:5703: com.hazelcast.core.HazelcastException: java.io.IOException: Connection refused to address erosb.eu/81.0.124.44:5703
     * May 10, 2022 9:31:10 AM com.hazelcast.client.impl.connection.ClientConnectionManager
     * WARNING: hz.client_1 [dev] [5.1-SNAPSHOT] Unable to get live cluster connection, retry in 11369 ms, attempt: 51, cluster connect timeout: INFINITE, max backoff: 30000 ms
     */
    @Test
    public void noMemberOnAddress() {
        ClientConfig config = newClientConfig();
        config.getListenerConfigs().add(new ListenerConfig().setImplementation(new MembershipListener() {
            @Override
            public void memberAdded(MembershipEvent membershipEvent) {
                System.out.println("added");
            }

            @Override
            public void memberRemoved(MembershipEvent membershipEvent) {
                System.out.println("removed");
            }
        }));
        config.getListenerConfigs().add(new ListenerConfig().setImplementation(new ClientConnectionProcessListener() {

            @Override
            public void attemptingToConnectToAddress(Address address) {
                System.out.println("attempt to connect: " + address);
            }

            @Override
            public void connectionAttemptFailed(Object target) {
                System.out.println("connection failed to " + target);
            }
        }));
//        startMember(new Config());
//        startMember(new Config());
        HazelcastClientInstanceImpl client = ((HazelcastClientProxy) HazelcastClient.newHazelcastClient(config)).client;
        client.getConnectionManager().addConnectionListener(new ConnectionListener<ClientConnection>() {
            @Override
            public void connectionAdded(ClientConnection connection) {

            }

            @Override
            public void connectionRemoved(ClientConnection connection) {

            }
        });

        System.out.println("meh");
        client.getMap("map-1").put(1, 2);
    }

    /**
     * Logs:
     *
     * May 10, 2022 2:48:37 PM com.hazelcast.client.impl.connection.ClientConnectionManager
     * INFO: hz.client_1 [dev] [5.1-SNAPSHOT] Trying to connect to [localhost]:6000
     * May 10, 2022 2:48:37 PM com.hazelcast.client.impl.connection.ClientConnectionManager
     * WARNING: hz.client_1 [dev] [5.1-SNAPSHOT] Exception during initial connection to [localhost]:6000: com.hazelcast.core.HazelcastException: java.io.IOException: Connection refused to address localhost/127.0.0.1:6000
     */
    @Test
    public void portIsClosed() {
        ClientConfig config = newClientConfig();
        config.getNetworkConfig().addAddress("localhost:6000");
        HazelcastClient.newHazelcastClient(config);
    }

    /**
     * com.hazelcast.client.AuthenticationException: Authentication failed. The configured cluster name on the client (see ClientConfig.setClusterName()) does not match the one configured in the cluster or the credentials set in the Client security config could not be authenticated
     * 	at com.hazelcast.client.impl.connection.tcp.TcpClientConnectionManager.checkAuthenticationResponse(TcpClientConnectionManager.java:990)
     * 	at com.hazelcast.client.impl.connection.tcp.TcpClientConnectionManager.onAuthenticated(TcpClientConnectionManager.java:893)
     * 	at com.hazelcast.client.impl.connection.tcp.TcpClientConnectionManager.getOrConnectToAddress(TcpClientConnectionManager.java:614)
     * 	at com.hazelcast.client.impl.connection.tcp.TcpClientConnectionManager.lambda$doConnectToCandidateCluster$3(TcpClientConnectionManager.java:504)
     * 	at com.hazelcast.client.impl.connection.tcp.TcpClientConnectionManager.connect(TcpClientConnectionManager.java:458)
     * 	at com.hazelcast.client.impl.connection.tcp.TcpClientConnectionManager.doConnectToCandidateCluster(TcpClientConnectionManager.java:504)
     * 	at com.hazelcast.client.impl.connection.tcp.TcpClientConnectionManager.doConnectToCluster(TcpClientConnectionManager.java:410)
     * 	at com.hazelcast.client.impl.connection.tcp.TcpClientConnectionManager.connectToCluster(TcpClientConnectionManager.java:371)
     * 	at com.hazelcast.client.impl.clientside.HazelcastClientInstanceImpl.start(HazelcastClientInstanceImpl.java:384)
     * 	at com.hazelcast.client.HazelcastClient.constructHazelcastClient(HazelcastClient.java:460)
     * 	at com.hazelcast.client.HazelcastClient.newHazelcastClientInternal(HazelcastClient.java:416)
     * 	at com.hazelcast.client.HazelcastClient.newHazelcastClient(HazelcastClient.java:136)
     * 	at com.hazelcast.client.DummyClient.clusterNameMismatch(DummyClient.java:69)
     * 	at java.base/jdk.internal.reflect.NativeMethodAccessorImpl.invoke0(Native Method)
     * 	at java.base/jdk.internal.reflect.NativeMethodAccessorImpl.invoke(NativeMethodAccessorImpl.java:62)
     * 	at java.base/jdk.internal.reflect.DelegatingMethodAccessorImpl.invoke(DelegatingMethodAccessorImpl.java:43)
     * 	at java.base/java.lang.reflect.Method.invoke(Method.java:566)
     * 	at org.junit.runners.model.FrameworkMethod$1.runReflectiveCall(FrameworkMethod.java:59)
     * 	at org.junit.internal.runners.model.ReflectiveCallable.run(ReflectiveCallable.java:12)
     * 	at org.junit.runners.model.FrameworkMethod.invokeExplosively(FrameworkMethod.java:56)
     * 	at org.junit.internal.runners.statements.InvokeMethod.evaluate(InvokeMethod.java:17)
     * 	at org.junit.runners.ParentRunner$3.evaluate(ParentRunner.java:306)
     * 	at org.junit.runners.BlockJUnit4ClassRunner$1.evaluate(BlockJUnit4ClassRunner.java:100)
     * 	at org.junit.runners.ParentRunner.runLeaf(ParentRunner.java:366)
     * 	at org.junit.runners.BlockJUnit4ClassRunner.runChild(BlockJUnit4ClassRunner.java:103)
     * 	at org.junit.runners.BlockJUnit4ClassRunner.runChild(BlockJUnit4ClassRunner.java:63)
     * 	at org.junit.runners.ParentRunner$4.run(ParentRunner.java:331)
     * 	at org.junit.runners.ParentRunner$1.schedule(ParentRunner.java:79)
     * 	at org.junit.runners.ParentRunner.runChildren(ParentRunner.java:329)
     * 	at org.junit.runners.ParentRunner.access$100(ParentRunner.java:66)
     * 	at org.junit.runners.ParentRunner$2.evaluate(ParentRunner.java:293)
     * 	at org.junit.runners.ParentRunner$3.evaluate(ParentRunner.java:306)
     * 	at org.junit.runners.ParentRunner.run(ParentRunner.java:413)
     * 	at org.junit.runner.JUnitCore.run(JUnitCore.java:137)
     * 	at com.intellij.junit4.JUnit4IdeaTestRunner.startRunnerWithArgs(JUnit4IdeaTestRunner.java:69)
     * 	at com.intellij.rt.junit.IdeaTestRunner$Repeater.startRunnerWithArgs(IdeaTestRunner.java:33)
     * 	at com.intellij.rt.junit.JUnitStarter.prepareStreamsAndStart(JUnitStarter.java:235)
     * 	at com.intellij.rt.junit.JUnitStarter.main(JUnitStarter.java:54)
     *
     * May 11, 2022 2:23:27 PM com.hazelcast.client.impl.connection.ClientConnectionManager
     * WARNING: hz.client_1 [something-not-dev] [5.1-SNAPSHOT] Exception during initial connection to [127.0.0.1]:5701: com.hazelcast.client.AuthenticationException: Authentication failed. The configured cluster name on the client (see ClientConfig.setClusterName()) does not match the one configured in the cluster or the credentials set in the Client security config could not be authenticated
     * May 11, 2022 2:23:27 PM com.hazelcast.internal.server.tcp.TcpServerConnection
     */
    @Test
    public void clusterNameMismatch() {
        Config memberConfig = new Config();
        startMember(memberConfig);
        ClientConfig config = newClientConfig();
        config.getConnectionStrategyConfig().setAsyncStart(true);
        config.setClusterName("something-not-dev");
        HazelcastClient.newHazelcastClient(config);
    }

    /**
     * Does not fail
     */
    @Test
    public void connectsOnlyToSomeMembers() {
        Config member1Config = new Config();
        member1Config.getNetworkConfig().setPort(5701);
        startMember(member1Config);
        Config member2Config = new Config();
        member2Config.getNetworkConfig().setPort(7001);
        startMember(member2Config);
        ClientConfig config = newClientConfig();
        config.getNetworkConfig().setAddresses(asList("localhost:5701", "localhost:5702"));
        HazelcastClient.newHazelcastClient(config);
    }

    private void startMember(Config config) {
        new Thread(() ->
            Hazelcast.newHazelcastInstance(config)
        ).start();
    }

    @NotNull
    private ClientConfig newClientConfig() {
        ClientConfig config = new ClientConfig();
        config.getConnectionStrategyConfig().getConnectionRetryConfig().setClusterConnectTimeoutMillis(10_000);
        return config;
    }
}
