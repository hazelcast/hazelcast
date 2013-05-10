package com.hazelcast.collection;

import com.hazelcast.clientv2.AuthenticationRequest;
import com.hazelcast.clientv2.ClientPortableHook;
import com.hazelcast.collection.operations.clientv2.AddAllRequest;
import com.hazelcast.collection.set.ObjectSetProxy;
import com.hazelcast.config.Config;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.instance.GroupProperties;
import com.hazelcast.nio.serialization.*;
import com.hazelcast.security.UsernamePasswordCredentials;
import org.junit.*;
import org.junit.runner.RunWith;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * @ali 5/10/13
 */
@RunWith(com.hazelcast.util.RandomBlockJUnit4ClassRunner.class)
public class CollectionBinaryClientTest {


    static final String name = "test";
    static final CollectionProxyId mmProxyId = new CollectionProxyId(name, null, CollectionProxyType.MULTI_MAP);
    static final CollectionProxyId setProxyId = new CollectionProxyId(ObjectSetProxy.COLLECTION_SET_NAME, name, CollectionProxyType.SET);
    static Data dataKey = null;
    static HazelcastInstance hz = null;
    static SerializationService ss = null;
    Client c = null;

    @BeforeClass
    public static void init() {
        Config config = new Config();
        hz = Hazelcast.newHazelcastInstance(config);
        ss = new SerializationServiceImpl(0);
        dataKey = ss.toData(name);
    }

    @AfterClass
    public static void destroy() {
        Hazelcast.shutdownAll();
    }

    @Before
    public void startClient() throws IOException {
        c = new Client();
        c.auth();
    }

    @After
    public void closeClient() throws IOException {
        hz.getMultiMap(name).clear();
        hz.getSet(name).clear();
        c.close();
    }

    @Test
    public void testAddAll() throws IOException {

        List<Data> list = new ArrayList<Data>();
        list.add(ss.toData("item1"));
        list.add(ss.toData("item2"));
        list.add(ss.toData("item3"));
        list.add(ss.toData("item4"));


        int threadId = (int)Thread.currentThread().getId();
        c.send(new AddAllRequest(setProxyId, dataKey, threadId, list));
        Object result = c.receive();
        assertTrue((Boolean) result);
        int size = hz.getSet(name).size();
        assertEquals(size, list.size());

    }

    static class Client {
        final Socket socket = new Socket();
        final SerializationService ss = new SerializationServiceImpl(0);
        final ObjectDataInputStream in;
        final ObjectDataOutputStream out;

        Client() throws IOException {
            socket.connect(new InetSocketAddress(InetAddress.getByName("127.0.0.1"), 5701));
            OutputStream outputStream = socket.getOutputStream();
            outputStream.write(new byte[]{'C', 'B', '1'});
            outputStream.flush();
            in = ss.createObjectDataInputStream(new BufferedInputStream(socket.getInputStream()));
            out = ss.createObjectDataOutputStream(new BufferedOutputStream(outputStream));

            ClassDefinitionBuilder builder = new ClassDefinitionBuilder(ClientPortableHook.ID, ClientPortableHook.PRINCIPAL);
            builder.addUTFField("uuid").addUTFField("ownerUuid");
            ss.getSerializationContext().registerClassDefinition(builder.build());
        }

        void auth() throws IOException {
            AuthenticationRequest auth = new AuthenticationRequest(new UsernamePasswordCredentials("dev", "dev-pass"));
            send(auth);
            Object o = receive();
            System.err.println("AUTH -> " + o);
        }

        void send(Object o) throws IOException {
            final Data data = ss.toData(o);
            data.writeData(out);
            out.flush();
        }

        Object receive() throws IOException {
            Data responseData = new Data();
            responseData.readData(in);
            return ss.toObject(responseData);
        }

        void close() throws IOException {
            socket.close();
        }
    }

    static {
        System.setProperty("hazelcast.logging.type", "log4j");
        System.setProperty("hazelcast.version.check.enabled", "false");
        System.setProperty("hazelcast.socket.bind.any", "false");
        System.setProperty("hazelcast.local.localAddress", "127.0.0.1");
        System.setProperty("java.net.preferIPv4Stack", "true");
        System.setProperty(GroupProperties.PROP_WAIT_SECONDS_BEFORE_JOIN, "0");
//        System.setProperty("java.net.preferIPv6Addresses", "true");
//        System.setProperty("hazelcast.prefer.ipv4.stack", "false");
    }
}
