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

import java.io.IOException;
import java.util.concurrent.Future;

public class ListenerThread extends Thread {
    final private ListenerResponseHandler listenerResponseHandler;
    final private Protocol request;
    final private Connection connection;
    final private SerializationService ss;
    final private ProtocolWriter writer;
    final private ProtocolReader reader;
    volatile boolean running;

    public ListenerThread(String name, Protocol request, ListenerResponseHandler listenerResponseHandler, Connection connection, SerializationService ss) {
        super(name);
        this.request = request;
        this.listenerResponseHandler = listenerResponseHandler;
        this.connection = connection;
        this.ss = ss;
        writer = new ProtocolWriter(ss);
        reader = new ProtocolReader(ss);
        running = true;
    }


    public void run() {
        try {
            System.out.println("Thread " + getName() + " is running");
            request.onEnqueue();
            writer.write(connection, request);
            writer.flush(connection);
            Future<Protocol> f = null;
            while (running) {
                Protocol response = reader.read(connection);
                listenerResponseHandler.handleResponse(response, ss);
            }
            cleanup();
        } catch (Exception e) {
            if (!running)
                return;
            else
                listenerResponseHandler.onError(e);
        } finally {
            cleanup();
        }
    }

    private void cleanup() {
        try {
            connection.close();
        } catch (IOException e) {
        }
    }

    public void stopListening() {
        running = false;
        try {
            this.connection.close();
        } catch (IOException e) {
        }
    }
}
