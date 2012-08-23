/*
 * Copyright (c) 2008-2012, Hazel Bilisim Ltd. All Rights Reserved.
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
 */

package com.hazelcast.impl.ascii;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.impl.OutOfMemoryErrorDispatcher;
import com.hazelcast.util.Clock;
import com.hazelcast.impl.Node;
import com.hazelcast.impl.ThreadContext;
import com.hazelcast.impl.ascii.memcache.*;
import com.hazelcast.impl.ascii.rest.HttpDeleteCommandProcessor;
import com.hazelcast.impl.ascii.rest.HttpGetCommandProcessor;
import com.hazelcast.impl.ascii.rest.HttpPostCommandProcessor;
import com.hazelcast.impl.ascii.rest.RestValue;
import com.hazelcast.logging.ILogger;
import com.hazelcast.nio.ascii.SocketTextWriter;
import com.hazelcast.util.Clock;
import com.hazelcast.util.SimpleBlockingQueue;

import java.nio.ByteBuffer;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.logging.Level;

import static com.hazelcast.impl.ascii.TextCommandConstants.TextCommandType.*;

public class TextCommandServiceImpl implements TextCommandService, TextCommandConstants {
    private final Node node;
//    private final ParallelExecutor parallelExecutor;
    private final Executor parallelExecutor;
    private final TextCommandProcessor[] textCommandProcessors = new TextCommandProcessor[100];
    private final HazelcastInstance hazelcast;
    private final AtomicLong gets = new AtomicLong();
    private final AtomicLong sets = new AtomicLong();
    private final AtomicLong deletes = new AtomicLong();
    private final AtomicLong getHits = new AtomicLong();
    private final long startTime = Clock.currentTimeMillis();
    private volatile ResponseThreadRunnable responseThreadRunnable;
    private volatile boolean running = true;
    private final ILogger logger;

    public TextCommandServiceImpl(Node node) {
        this.node = node;
        this.hazelcast = node.hazelcastInstance;
        this.logger = node.getLogger(this.getClass().getName());
//        this.parallelExecutor = this.node.executorManager.newParallelExecutor(40);
        this.parallelExecutor = this.node.nodeService.getExecutorService();
        textCommandProcessors[GET.getValue()] = new GetCommandProcessor(this, true);
        textCommandProcessors[PARTIAL_GET.getValue()] = new GetCommandProcessor(this, false);
        textCommandProcessors[SET.getValue()] = new SetCommandProcessor(this);
        textCommandProcessors[ADD.getValue()] = new SetCommandProcessor(this);
        textCommandProcessors[REPLACE.getValue()] = new SetCommandProcessor(this);
        textCommandProcessors[GET_END.getValue()] = new NoOpCommandProcessor(this);
        textCommandProcessors[DELETE.getValue()] = new DeleteCommandProcessor(this);
        textCommandProcessors[QUIT.getValue()] = new SimpleCommandProcessor(this);
        textCommandProcessors[STATS.getValue()] = new StatsCommandProcessor(this);
        textCommandProcessors[UNKNOWN.getValue()] = new ErrorCommandProcessor(this);
        textCommandProcessors[ERROR_CLIENT.getValue()] = new ErrorCommandProcessor(this);
        textCommandProcessors[ERROR_SERVER.getValue()] = new ErrorCommandProcessor(this);
        textCommandProcessors[HTTP_GET.getValue()] = new HttpGetCommandProcessor(this);
        textCommandProcessors[HTTP_POST.getValue()] = new HttpPostCommandProcessor(this);
        textCommandProcessors[HTTP_PUT.getValue()] = new HttpPostCommandProcessor(this);
        textCommandProcessors[HTTP_DELETE.getValue()] = new HttpDeleteCommandProcessor(this);
        textCommandProcessors[NO_OP.getValue()] = new NoOpCommandProcessor(this);
    }

    public Node getNode() {
        return node;
    }

    public Stats getStats() {
        Stats stats = new Stats();
        stats.uptime = (int) ((Clock.currentTimeMillis() - startTime) / 1000);
//        stats.threads = parallelExecutor.getActiveCount();
//        stats.waiting_requests = parallelExecutor.getPoolSize();
        stats.cmd_get = gets.get();
        stats.cmd_set = sets.get();
        stats.cmd_delete = deletes.get();
        stats.get_hits = getHits.get();
        stats.get_misses = gets.get() - getHits.get();
        stats.curr_connections = node.connectionManager.getCurrentClientConnections();
        stats.total_connections = node.connectionManager.getAllTextConnections();
        return stats;
    }

    public long incrementDeleteCount() {
        return deletes.incrementAndGet();
    }

    public long incrementGetCount() {
        return gets.incrementAndGet();
    }

    public long incrementSetCount() {
        return sets.incrementAndGet();
    }

    public long incrementHitCount() {
        return getHits.incrementAndGet();
    }

    public void processRequest(TextCommand command) {
        if (responseThreadRunnable == null) {
            synchronized (this) {
                if (responseThreadRunnable == null) {
                    responseThreadRunnable = new ResponseThreadRunnable();
                    Thread thread = new Thread(node.threadGroup, responseThreadRunnable, "hz.ascii.service.response.thread");
                    thread.start();
                }
            }
        }
        parallelExecutor.execute(new CommandExecutor(command));
    }

    public Object get(String mapName, String key) {
        return hazelcast.getMap(mapName).get(key);
    }

    public int getAdjustedTTLSeconds(int ttl) {
        if (ttl <= MONTH_SECONDS) {
            return ttl;
        } else {
            return ttl - (int) (Clock.currentTimeMillis() / 1000);
        }
    }

    public byte[] getByteArray(String mapName, String key) {
        Object value = hazelcast.getMap(mapName).get(key);
        byte[] result = null;
        if (value != null) {
            if (value instanceof RestValue) {
                RestValue restValue = (RestValue) value;
                result = restValue.getValue();
            } else if (value instanceof byte[]) {
                result = (byte[]) value;
            } else {
                result = ThreadContext.get().toByteArray(value);
            }
        }
        return result;
    }

    public Object put(String mapName, String key, Object value, int ttlSeconds) {
        return hazelcast.getMap(mapName).put(key, value, ttlSeconds, TimeUnit.SECONDS);
    }

    public Object putIfAbsent(String mapName, String key, Object value, int ttlSeconds) {
        return hazelcast.getMap(mapName).putIfAbsent(key, value, ttlSeconds, TimeUnit.SECONDS);
    }

    public Object replace(String mapName, String key, Object value) {
        return hazelcast.getMap(mapName).replace(key, value);
    }

    public Object delete(String mapName, String key) {
        return hazelcast.getMap(mapName).remove(key);
    }

    public boolean offer(String queueName, Object value) {
        return hazelcast.getQueue(queueName).offer(value);
    }

    public Object poll(String queueName, int seconds) {
        try {
            return hazelcast.getQueue(queueName).poll(seconds, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            return null;
        }
    }

    public Object poll(String queueName) {
        return hazelcast.getQueue(queueName).poll();
    }

    class CommandExecutor implements Runnable {
        final TextCommand command;

        CommandExecutor(TextCommand command) {
            this.command = command;
        }

        public void run() {
            try {
                TextCommandType type = command.getType();
                textCommandProcessors[type.getValue()].handle(command);
            } catch (Throwable e) {
                logger.log(Level.WARNING, e.getMessage(), e);
            }
        }
    }

    public void sendResponse(TextCommand textCommand) {
        if (!textCommand.shouldReply() || textCommand.getRequestId() == -1) {
            throw new RuntimeException("Shouldn't reply " + textCommand);
        }
        responseThreadRunnable.sendResponse(textCommand);
    }

    class ResponseThreadRunnable implements Runnable {
        private final BlockingQueue<TextCommand> blockingQueue = new SimpleBlockingQueue<TextCommand>();
        private final Object stopObject = new Object();

        public void sendResponse(TextCommand textCommand) {
            blockingQueue.offer(textCommand);
        }

        public void run() {
            while (running) {
                try {
                    TextCommand textCommand = blockingQueue.take();
                    if (TextCommandConstants.TextCommandType.STOP == textCommand.getType()) {
                        synchronized (stopObject) {
                            stopObject.notify();
                        }
                    } else {
                        SocketTextWriter socketTextWriter = textCommand.getSocketTextWriter();
                        socketTextWriter.enqueue(textCommand);
                    }
                } catch (InterruptedException e) {
                    return;
                } catch (OutOfMemoryError e) {
                    OutOfMemoryErrorDispatcher.onOutOfMemory(e);
                    throw e;
                }
            }
        }

        void stop() {
            running = false;
            synchronized (stopObject) {
                try {
                    blockingQueue.offer(new AbstractTextCommand(TextCommandConstants.TextCommandType.STOP) {
                        public boolean doRead(ByteBuffer cb) {
                            return true;
                        }

                        public boolean writeTo(ByteBuffer bb) {
                            return true;
                        }
                    });
                    //noinspection WaitNotInLoop
                    stopObject.wait(1000);
                } catch (Exception ignored) {
                }
            }
        }
    }

    public void stop() {
        if (responseThreadRunnable != null) {
            responseThreadRunnable.stop();
        }
    }
}
