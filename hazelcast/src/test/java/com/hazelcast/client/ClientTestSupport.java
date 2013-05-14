package com.hazelcast.client;

import com.hazelcast.nio.serialization.*;
import com.hazelcast.security.UsernamePasswordCredentials;
import org.junit.After;
import org.junit.Before;
import org.junit.runner.RunWith;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.Socket;

/**
 * @mdogan 5/14/13
 */
@RunWith(com.hazelcast.util.RandomBlockJUnit4ClassRunner.class)
public abstract class ClientTestSupport {

    private Client client;

    @Before
    public final void startClient() throws IOException {
        client = new Client();
        client.auth();
    }

    @After
    public final void closeClient() throws IOException {
        client.close();
    }

    protected final Client client() {
        return client;
    }

    public final static class Client {
        final Socket socket = new Socket();
        final SerializationService ss = new SerializationServiceImpl(0);
        final ObjectDataInputStream in;
        final ObjectDataOutputStream out;

        public Client() throws IOException {
            socket.connect(new InetSocketAddress(InetAddress.getByName("127.0.0.1"), 5701));
            OutputStream outputStream = socket.getOutputStream();
            outputStream.write(new byte[]{'C', 'B', '1'});
            outputStream.flush();
            in = ss.createObjectDataInputStream(new BufferedInputStream(socket.getInputStream()));
            out = ss.createObjectDataOutputStream(new BufferedOutputStream(outputStream));
        }

        public void auth() throws IOException {
            AuthenticationRequest auth = new AuthenticationRequest(new UsernamePasswordCredentials("dev", "dev-pass"));
            send(auth);
            receive();
        }

        public void send(Object o) throws IOException {
            final Data data = ss.toData(o);
            data.writeData(out);
            out.flush();
        }

        public Object receive() throws IOException {
            Data responseData = new Data();
            responseData.readData(in);
            return ss.toObject(responseData);
        }

        public void close() throws IOException {
            socket.close();
        }
    }
}
