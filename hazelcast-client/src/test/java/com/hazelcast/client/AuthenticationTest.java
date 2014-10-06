package com.hazelcast.client;

import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.config.Config;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.Member;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.HashSet;
import java.util.Set;

import static org.junit.Assert.assertEquals;

@RunWith(HazelcastSerialClassRunner.class)
@Category(QuickTest.class)
public class AuthenticationTest extends HazelcastTestSupport {

    @Before
    @After
    public void cleanup(){
        Hazelcast.shutdownAll();
        HazelcastClient.shutdownAll();
    }

    @Test
    public void test_whenDefaultConfigurationUsed_thenAuthenticationSuccess() throws Throwable {
        HazelcastInstance hz = Hazelcast.newHazelcastInstance();

        HazelcastInstance client = HazelcastClient.newHazelcastClient();
        Set<Member> expected = new HashSet<Member>();
        expected.add(hz.getCluster().getLocalMember());

        assertEquals(expected, client.getCluster().getMembers());
    }

    @Test
    public void test_authenticationSuccess() throws Throwable {
        Config config = new Config();
        config.getGroupConfig().setName("somegroup");
        config.getGroupConfig().setPassword("somepassword");
        HazelcastInstance hz = Hazelcast.newHazelcastInstance(config);

        ClientConfig clientConfig = new ClientConfig();
        clientConfig.getGroupConfig()
                .setName(config.getGroupConfig().getName())
                .setPassword(config.getGroupConfig().getPassword());

        HazelcastInstance client = HazelcastClient.newHazelcastClient(clientConfig);
        Set<Member> expected = new HashSet<Member>();
        expected.add(hz.getCluster().getLocalMember());

        assertEquals(expected, client.getCluster().getMembers());
    }

    @Ignore
    @Test(expected = AuthenticationException.class)
    public void test_wrongPassword() throws Throwable {
        Config config = new Config();
        config.getGroupConfig().setName("somegroup");
        config.getGroupConfig().setPassword("somepassword");
        HazelcastInstance hz = Hazelcast.newHazelcastInstance(config);

        ClientConfig clientConfig = new ClientConfig();
        clientConfig.getGroupConfig()
                .setName(config.getGroupConfig().getName())
                .setPassword("otherpassword");

        HazelcastClient.newHazelcastClient(clientConfig);
    }

    @Ignore
    @Test//todo: we need to determine the right exception
    public void test_wrongGroupName() throws Throwable {
        Config config = new Config();
        config.getGroupConfig().setName("somegroup");
        config.getGroupConfig().setPassword("somepassword");
        HazelcastInstance hz = Hazelcast.newHazelcastInstance();

        ClientConfig clientConfig = new ClientConfig();
        clientConfig.getGroupConfig()
                .setName("othergroupname")
                .setPassword(config.getGroupConfig().getPassword());

        HazelcastInstance client = HazelcastClient.newHazelcastClient(clientConfig);
    }

}
