/*
 * Copyright (c) 2008-2010, Hazel Ltd. All Rights Reserved.
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
 *
 */

package com.hazelcast.client.proxy.listener;

import com.hazelcast.client.Connection;
import com.hazelcast.client.ProtocolReader;
import com.hazelcast.client.ProtocolWriter;
import com.hazelcast.nio.Protocol;
import com.hazelcast.nio.serialization.SerializationService;

import java.io.EOFException;
import java.io.IOException;
import java.util.concurrent.*;

public class ListenerThread extends Thread {
    final private ListenerResponseHandler listenerResponseHandler;
    final private Protocol request;
    final private Connection connection;
    final private SerializationService ss;
    final private ProtocolWriter writer;
    final private ProtocolReader reader;

    public ListenerThread(Protocol request, ListenerResponseHandler listenerResponseHandler, Connection connection, SerializationService ss) {
        this.request = request;
        this.listenerResponseHandler = listenerResponseHandler;
        this.connection = connection;
        this.ss = ss;
        writer = new ProtocolWriter(ss);
        reader = new ProtocolReader(ss);
    }

    public void run() {
        ExecutorService ex = Executors.newFixedThreadPool(1);
        try {
            request.onEnqueue();
            writer.write(connection, request);
            writer.flush(connection);
            Future<Protocol> f = null;
            while (!Thread.interrupted()) {
                if (f == null) {
                    f = ex.submit(new Callable<Protocol>() {
                        public Protocol call() throws Exception {
                            return reader.read(connection);
                        }
                    });
                }
                Protocol response = f.get(1, TimeUnit.SECONDS);
                if (response == null) continue;
                f = null;
                listenerResponseHandler.handleResponse(response, ss);
            }
            connection.close();
            ex.shutdown();
            System.out.println("Closed connection and thus removed the listener");
        } catch (EOFException e) {
            e.printStackTrace();
            //Means that the connection is broken. The best is to re add the listener and end the current thread.
//                addEntryListener(listener, key, includeValue);
            return; // will end the current thread
        } catch (IOException e) {
            e.printStackTrace();
            throw new RuntimeException(e);
        } catch (InterruptedException e) {
            try {
                connection.close();
            } catch (IOException e1) {
            }
            ex.shutdown();
            System.out.println("Closed connection and thus removed the listener");
            return;
        } catch (ExecutionException e) {
            e.printStackTrace();
        } catch (TimeoutException e) {
            e.printStackTrace();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
