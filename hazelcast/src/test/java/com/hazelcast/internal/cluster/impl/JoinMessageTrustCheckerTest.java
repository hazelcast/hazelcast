package com.hazelcast.internal.cluster.impl;

import com.hazelcast.nio.Address;
import com.hazelcast.nio.Packet;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import com.hazelcast.version.MemberVersion;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;


import java.net.InetAddress;
import java.net.UnknownHostException;

import static java.util.Collections.EMPTY_SET;
import static java.util.Collections.singleton;
import static org.junit.Assert.*;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelTest.class})
public class JoinMessageTrustCheckerTest extends HazelcastTestSupport {

    @Test
    public void givenNoInterfaceIsConfigured_whenMessageArrives_thenTrust() throws UnknownHostException {
        JoinMessageTrustChecker joinMessageTrustChecker = new JoinMessageTrustChecker(EMPTY_SET);
        JoinMessage message = createJoinMessage("127.0.0.1");
        assertTrue(joinMessageTrustChecker.isTrusted(message));
    }

    @Test
    public void givenInterfaceIsConfigured_whenMessageWithMatchingHost_thenTrust() throws UnknownHostException {
        JoinMessageTrustChecker joinMessageTrustChecker = new JoinMessageTrustChecker(singleton("127.0.0.1"));
        JoinMessage message = createJoinMessage("127.0.0.1");
        assertTrue(joinMessageTrustChecker.isTrusted(message));
    }

    @Test
    public void givenInterfaceIsConfigured_whenMessageWithNonMatchingHost_thenDoNotTrust() throws UnknownHostException {
        JoinMessageTrustChecker joinMessageTrustChecker = new JoinMessageTrustChecker(singleton("127.0.0.2"));
        JoinMessage message = createJoinMessage("127.0.0.1");
        assertFalse(joinMessageTrustChecker.isTrusted(message));
    }

    @Test
    public void givenInterfaceRangeIsConfigured_whenMessageWithMatchingHost_thenTrust() throws UnknownHostException {
        JoinMessageTrustChecker joinMessageTrustChecker = new JoinMessageTrustChecker(singleton("127.0.0.1-100"));
        JoinMessage message = createJoinMessage("127.0.0.2");
        assertTrue(joinMessageTrustChecker.isTrusted(message));
    }

    @Test
    public void givenInterfaceRangeIsConfigured_whenMessageWithNonMatchingHost_thenDoNotTrust() throws UnknownHostException {
        JoinMessageTrustChecker joinMessageTrustChecker = new JoinMessageTrustChecker(singleton("127.0.0.1-100"));
        JoinMessage message = createJoinMessage("127.0.0.101");
        assertFalse(joinMessageTrustChecker.isTrusted(message));
    }

    private JoinMessage createJoinMessage(String ip) throws UnknownHostException {
        InetAddress inetAddress = InetAddress.getByName(ip);
        Address address = new Address(inetAddress, 5701);
        ConfigCheck configCheck = new ConfigCheck();
        return new JoinMessage(Packet.VERSION, 0, MemberVersion.UNKNOWN, address, "uuid", false, configCheck);
    }

}