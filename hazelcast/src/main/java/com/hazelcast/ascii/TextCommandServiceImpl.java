/*
 * Copyright (c) 2008-2013, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.ascii;

import com.hazelcast.ascii.memcache.*;
import com.hazelcast.ascii.rest.HttpDeleteCommandProcessor;
import com.hazelcast.ascii.rest.HttpGetCommandProcessor;
import com.hazelcast.ascii.rest.HttpPostCommandProcessor;
import com.hazelcast.ascii.rest.RestValue;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IMap;
import com.hazelcast.instance.Node;
import com.hazelcast.instance.OutOfMemoryErrorDispatcher;
import com.hazelcast.logging.ILogger;
import com.hazelcast.nio.ascii.SocketTextWriter;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.util.Clock;

import java.nio.ByteBuffer;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import static com.hazelcast.ascii.TextCommandConstants.TextCommandType.*;

public class TextCommandServiceImpl implements TextCommandService, TextCommandConstants {
    private final Node node;
    private final TextCommandProcessor[] textCommandProcessors = new TextCommandProcessor[100];
    private final HazelcastInstance hazelcast;
    private final AtomicLong sets = new AtomicLong();
    private final AtomicLong touches = new AtomicLong();
    private final AtomicLong getHits = new AtomicLong();
    private final AtomicLong getMisses = new AtomicLong();
    private final AtomicLong deleteMisses = new AtomicLong();
    private final AtomicLong deleteHits = new AtomicLong();
    private final AtomicLong incrementHits = new AtomicLong();
    private final AtomicLong incrementMisses = new AtomicLong();
    private final AtomicLong decrementHits = new AtomicLong();
    private final AtomicLong decrementMisses = new AtomicLong();
    private final long startTime = Clock.currentTimeMillis();
    private final ILogger logger;
    private volatile ResponseThreadRunnable responseThreadRunnable;
    private volatile boolean running = true;

    public TextCommandServiceImpl(Node node) {
        this.node = node;
        this.hazelcast = node.hazelcastInstance;
        this.logger = node.getLogger(this.getClass().getName());
        textCommandProcessors[GET.getValue()] = new GetCommandProcessor(this, true);
        textCommandProcessors[PARTIAL_GET.getValue()] = new GetCommandProcessor(this, false);
        textCommandProcessors[SET.getValue()] = new SetCommandProcessor(this);
        textCommandProcessors[APPEND.getValue()] = new SetCommandProcessor(this);
        textCommandProcessors[PREPEND.getValue()] = new SetCommandProcessor(this);
        textCommandProcessors[ADD.getValue()] = new SetCommandProcessor(this);
        textCommandProcessors[REPLACE.getValue()] = new SetCommandProcessor(this);
        textCommandProcessors[GET_END.getValue()] = new NoOpCommandProcessor(this);
        textCommandProcessors[DELETE.getValue()] = new DeleteCommandProcessor(this);
        textCommandProcessors[QUIT.getValue()] = new SimpleCommandProcessor(this);
        textCommandProcessors[STATS.getValue()] = new StatsCommandProcessor(this);
        textCommandProcessors[UNKNOWN.getValue()] = new ErrorCommandProcessor(this);
        textCommandProcessors[VERSION.getValue()] = new VersionCommandProcessor(this);
        textCommandProcessors[TOUCH.getValue()] = new TouchCommandProcessor(this);
        textCommandProcessors[INCREMENT.getValue()] = new IncrementCommandProcessor(this);
        textCommandProcessors[DECREMENT.getValue()] = new IncrementCommandProcessor(this);
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

    public byte[] toByteArray(Object value) {
        Data data = node.getSerializationService().toData(value);
        return data.getBuffer();
    }

    public Stats getStats() {
        Stats stats = new Stats();
        stats.uptime = (int) ((Clock.currentTimeMillis() - startTime) / 1000);
        stats.cmd_get = getMisses.get() + getHits.get();
        stats.cmd_set = sets.get();
        stats.cmd_touch = touches.get();
        stats.get_hits = getHits.get();
        stats.get_misses = getMisses.get();
        stats.delete_hits = deleteHits.get();
        stats.delete_misses = deleteMisses.get();
        stats.incr_hits = incrementHits.get();
        stats.incr_misses = incrementMisses.get();
        stats.decr_hits = decrementHits.get();
        stats.decr_misses = decrementMisses.get();
        stats.curr_connections = node.connectionManager.getCurrentClientConnections();
        stats.total_connections = node.connectionManager.getAllTextConnections();
        return stats;
    }

    public long incrementDeleteHitCount(int inc) {
        return deleteHits.addAndGet(inc);
    }

    public long incrementDeleteMissCount() {
        return deleteMisses.incrementAndGet();
    }

    public long incrementGetHitCount() {
        return getHits.incrementAndGet();
    }

    public long incrementGetMissCount() {
        return getMisses.incrementAndGet();
    }

    public long incrementSetCount() {
        return sets.incrementAndGet();
    }

    public long incrementIncHitCount() {
        return incrementHits.incrementAndGet();
    }

    public long incrementIncMissCount() {
        return incrementMisses.incrementAndGet();
    }

    public long incrementDecrHitCount() {
        return decrementHits.incrementAndGet();
    }

    public long incrementDecrMissCount() {
        return decrementMisses.incrementAndGet();
    }

    public long incrementTouchCount() {
        return touches.incrementAndGet();
    }

    public void processRequest(TextCommand command) {
        if (responseThreadRunnable == null) {
            synchronized (this) {
                if (responseThreadRunnable == null) {
                    responseThreadRunnable = new ResponseThreadRunnable();
                    Thread thread = new Thread(node.threadGroup, responseThreadRunnable, node.getThreadNamePrefix("ascii.service.response"));
                    thread.start();
                }
            }
        }
        node.nodeEngine.getExecutionService().execute("hz:text", new CommandExecutor(command));
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
                result = toByteArray(value);
            }
        }
        return result;
    }

    public Object put(String mapName, String key, Object value) {
        return hazelcast.getMap(mapName).put(key, value);
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

    public void lock(String mapName, String key) throws InterruptedException {
        if (!hazelcast.getMap(mapName).tryLock(key, 1, TimeUnit.MINUTES)) {
            throw new RuntimeException("Memcache client could not get the lock for map:" + mapName + " key:" + key + " in 1 minute");
        }
    }

    public void unlock(String mapName, String key) {
        hazelcast.getMap(mapName).unlock(key);
    }

    public void deleteAll(String mapName) {
        final IMap<Object, Object> map = hazelcast.getMap(mapName);
        map.clear();
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

    public void sendResponse(TextCommand textCommand) {
        if (!textCommand.shouldReply() || textCommand.getRequestId() == -1) {
            throw new RuntimeException("Shouldn't reply " + textCommand);
        }
        responseThreadRunnable.sendResponse(textCommand);
    }

    public void stop() {
        final ResponseThreadRunnable rtr = responseThreadRunnable;
        if (rtr != null) {
            rtr.stop();
        }
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
                logger.warning(e);
            }
        }
    }

    private class ResponseThreadRunnable implements Runnable {
        private final BlockingQueue<TextCommand> blockingQueue = new ArrayBlockingQueue<TextCommand>(200);
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
                        public boolean readFrom(ByteBuffer cb) {
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
}
