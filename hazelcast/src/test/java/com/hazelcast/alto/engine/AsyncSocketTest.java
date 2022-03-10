package com.hazelcast.alto.engine;

import com.hazelcast.internal.tpc.AsyncSocket;
import com.hazelcast.internal.tpc.Eventloop;
import org.junit.After;
import org.junit.Test;

import java.io.Closeable;
import java.util.ArrayList;
import java.util.List;

import static com.hazelcast.internal.nio.IOUtil.closeResources;
import static org.junit.Assert.assertTrue;

public abstract class AsyncSocketTest {

    public List<Closeable> closeables = new ArrayList<>();

    public abstract Eventloop createEventloop();

    public abstract AsyncSocket createAsyncSocket();

    @After
    public void after(){
        closeResources(closeables);
    }

    @Test
    public void close_whenNotActivated(){
        AsyncSocket socket = createAsyncSocket();
        socket.close();
        assertTrue(socket.isClosed());
    }


    @Test
    public void close_whenNotActivated_andAlreadyClosed(){
        AsyncSocket socket = createAsyncSocket();
        socket.close();
        socket.close();
        assertTrue(socket.isClosed());
    }

    @Test(expected = NullPointerException.class)
    public void activate_whenNull(){
        AsyncSocket socket = createAsyncSocket();
        socket.activate(null);
    }

    @Test(expected = IllegalStateException.class)
    public void activate_whenAlreadyActivated(){
        Eventloop eventloop1 = createEventloop();
        Eventloop eventloop2 = createEventloop();

        AsyncSocket socket = createAsyncSocket();
        eventloop1.start();
        eventloop2.start();

        socket.activate(eventloop1);
        socket.activate(eventloop2);
    }
}
