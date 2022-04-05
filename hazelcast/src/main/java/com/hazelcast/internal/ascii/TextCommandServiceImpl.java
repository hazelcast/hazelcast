/*
 * Copyright (c) 2008-2022, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.internal.ascii;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.instance.impl.Node;
import com.hazelcast.instance.impl.OutOfMemoryErrorDispatcher;
import com.hazelcast.internal.ascii.memcache.BulkGetCommandProcessor;
import com.hazelcast.internal.ascii.memcache.DeleteCommandProcessor;
import com.hazelcast.internal.ascii.memcache.EntryConverter;
import com.hazelcast.internal.ascii.memcache.ErrorCommandProcessor;
import com.hazelcast.internal.ascii.memcache.GetCommandProcessor;
import com.hazelcast.internal.ascii.memcache.IncrementCommandProcessor;
import com.hazelcast.internal.ascii.memcache.SetCommandProcessor;
import com.hazelcast.internal.ascii.memcache.SimpleCommandProcessor;
import com.hazelcast.internal.ascii.memcache.Stats;
import com.hazelcast.internal.ascii.memcache.StatsCommandProcessor;
import com.hazelcast.internal.ascii.memcache.TouchCommandProcessor;
import com.hazelcast.internal.ascii.memcache.VersionCommandProcessor;
import com.hazelcast.internal.ascii.rest.HttpDeleteCommandProcessor;
import com.hazelcast.internal.ascii.rest.HttpGetCommandProcessor;
import com.hazelcast.internal.ascii.rest.HttpHeadCommandProcessor;
import com.hazelcast.internal.ascii.rest.HttpPostCommandProcessor;
import com.hazelcast.internal.ascii.rest.RestCallCollector;
import com.hazelcast.internal.ascii.rest.RestValue;
import com.hazelcast.internal.nio.Protocols;
import com.hazelcast.internal.nio.ascii.TextEncoder;
import com.hazelcast.internal.serialization.Data;
import com.hazelcast.internal.server.Server;
import com.hazelcast.internal.server.ServerConnectionManager;
import com.hazelcast.internal.util.Clock;
import com.hazelcast.logging.ILogger;
import com.hazelcast.map.IMap;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

import java.nio.ByteBuffer;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import static com.hazelcast.instance.EndpointQualifier.MEMCACHE;
import static com.hazelcast.internal.ascii.TextCommandConstants.TextCommandType.ADD;
import static com.hazelcast.internal.ascii.TextCommandConstants.TextCommandType.APPEND;
import static com.hazelcast.internal.ascii.TextCommandConstants.TextCommandType.BULK_GET;
import static com.hazelcast.internal.ascii.TextCommandConstants.TextCommandType.DECREMENT;
import static com.hazelcast.internal.ascii.TextCommandConstants.TextCommandType.DELETE;
import static com.hazelcast.internal.ascii.TextCommandConstants.TextCommandType.ERROR_CLIENT;
import static com.hazelcast.internal.ascii.TextCommandConstants.TextCommandType.ERROR_SERVER;
import static com.hazelcast.internal.ascii.TextCommandConstants.TextCommandType.GET;
import static com.hazelcast.internal.ascii.TextCommandConstants.TextCommandType.GET_END;
import static com.hazelcast.internal.ascii.TextCommandConstants.TextCommandType.HTTP_DELETE;
import static com.hazelcast.internal.ascii.TextCommandConstants.TextCommandType.HTTP_GET;
import static com.hazelcast.internal.ascii.TextCommandConstants.TextCommandType.HTTP_HEAD;
import static com.hazelcast.internal.ascii.TextCommandConstants.TextCommandType.HTTP_POST;
import static com.hazelcast.internal.ascii.TextCommandConstants.TextCommandType.HTTP_PUT;
import static com.hazelcast.internal.ascii.TextCommandConstants.TextCommandType.INCREMENT;
import static com.hazelcast.internal.ascii.TextCommandConstants.TextCommandType.NO_OP;
import static com.hazelcast.internal.ascii.TextCommandConstants.TextCommandType.PREPEND;
import static com.hazelcast.internal.ascii.TextCommandConstants.TextCommandType.QUIT;
import static com.hazelcast.internal.ascii.TextCommandConstants.TextCommandType.REPLACE;
import static com.hazelcast.internal.ascii.TextCommandConstants.TextCommandType.SET;
import static com.hazelcast.internal.ascii.TextCommandConstants.TextCommandType.STATS;
import static com.hazelcast.internal.ascii.TextCommandConstants.TextCommandType.TOUCH;
import static com.hazelcast.internal.ascii.TextCommandConstants.TextCommandType.UNKNOWN;
import static com.hazelcast.internal.ascii.TextCommandConstants.TextCommandType.VERSION;
import static com.hazelcast.internal.util.EmptyStatement.ignore;
import static com.hazelcast.internal.util.ThreadUtil.createThreadName;
import static java.lang.Thread.currentThread;

@SuppressWarnings({"checkstyle:methodcount", "checkstyle:classdataabstractioncoupling"})
public class TextCommandServiceImpl implements TextCommandService {

    private static final int TEXT_COMMAND_PROCESSOR_SIZE = 100;
    private static final int MILLIS_TO_SECONDS = 1000;
    private static final long WAIT_TIME = 1000;

    private final TextCommandProcessor[] textCommandProcessors = new TextCommandProcessor[TEXT_COMMAND_PROCESSOR_SIZE];
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
    private final RestCallCollector restCallCollector = new RestCallCollector();

    private final Node node;
    private final HazelcastInstance hazelcast;
    private final ILogger logger;

    private volatile ResponseThreadRunnable responseThreadRunnable;
    private volatile boolean running = true;

    private final Object mutex = new Object();

    @SuppressWarnings("checkstyle:executablestatementcount")
    public TextCommandServiceImpl(Node node) {
        this.node = node;
        this.hazelcast = node.hazelcastInstance;
        this.logger = node.getLogger(this.getClass().getName());
        EntryConverter entryConverter = new EntryConverter(this, node.getLogger(EntryConverter.class));
        register(GET, new GetCommandProcessor(this, entryConverter));
        register(BULK_GET, new BulkGetCommandProcessor(this, entryConverter));
        register(SET, new SetCommandProcessor(this));
        register(APPEND, new SetCommandProcessor(this));
        register(PREPEND, new SetCommandProcessor(this));
        register(ADD, new SetCommandProcessor(this));
        register(REPLACE, new SetCommandProcessor(this));
        register(GET_END, new NoOpCommandProcessor(this));
        register(DELETE, new DeleteCommandProcessor(this));
        register(QUIT, new SimpleCommandProcessor(this));
        register(STATS, new StatsCommandProcessor(this));
        register(UNKNOWN, new ErrorCommandProcessor(this));
        register(VERSION, new VersionCommandProcessor(this));
        register(TOUCH, new TouchCommandProcessor(this));
        register(INCREMENT, new IncrementCommandProcessor(this, entryConverter));
        register(DECREMENT, new IncrementCommandProcessor(this, entryConverter));
        register(ERROR_CLIENT, new ErrorCommandProcessor(this));
        register(ERROR_SERVER, new ErrorCommandProcessor(this));
        register(HTTP_GET, new HttpGetCommandProcessor(this));
        register(HTTP_POST, new HttpPostCommandProcessor(this));
        register(HTTP_PUT, new HttpPostCommandProcessor(this));
        register(HTTP_DELETE, new HttpDeleteCommandProcessor(this));
        register(HTTP_HEAD, new HttpHeadCommandProcessor(this));
        register(NO_OP, new NoOpCommandProcessor(this));
    }

    protected void register(TextCommandConstants.TextCommandType type, TextCommandProcessor processor) {
        textCommandProcessors[type.getValue()] = processor;
    }

    @Override
    public Node getNode() {
        return node;
    }

    @Override
    public byte[] toByteArray(Object value) {
        Data data = node.getSerializationService().toData(value);
        return data.toByteArray();
    }

    @Override
    public Stats getStats() {
        Stats stats = new Stats();
        stats.setUptime((int) ((Clock.currentTimeMillis() - startTime) / MILLIS_TO_SECONDS));
        stats.setCmdGet(getMisses.get() + getHits.get());
        stats.setCmdSet(sets.get());
        stats.setCmdTouch(touches.get());
        stats.setGetHits(getHits.get());
        stats.setGetMisses(getMisses.get());
        stats.setDeleteHits(deleteHits.get());
        stats.setDeleteMisses(deleteMisses.get());
        stats.setIncrHits(incrementHits.get());
        stats.setIncrMisses(incrementMisses.get());
        stats.setDecrHits(decrementHits.get());
        stats.setDecrMisses(decrementMisses.get());
        Server server = node.getServer();
        ServerConnectionManager cm = server.getConnectionManager(MEMCACHE);
        int memcachedCount = cm == null ? 0 : cm.connectionCount(c -> Protocols.MEMCACHE.equals(c.getConnectionType()));
        stats.setCurrConnections(memcachedCount);
        stats.setTotalConnections(server.connectionCount());
        return stats;
    }

    @Override
    public long incrementDeleteHitCount(int inc) {
        return deleteHits.addAndGet(inc);
    }

    @Override
    public long incrementDeleteMissCount() {
        return deleteMisses.incrementAndGet();
    }

    @Override
    public long incrementGetHitCount() {
        return getHits.incrementAndGet();
    }

    @Override
    public long incrementGetMissCount() {
        return getMisses.incrementAndGet();
    }

    @Override
    public long incrementSetCount() {
        return sets.incrementAndGet();
    }

    @Override
    public long incrementIncHitCount() {
        return incrementHits.incrementAndGet();
    }

    @Override
    public long incrementIncMissCount() {
        return incrementMisses.incrementAndGet();
    }

    @Override
    public long incrementDecrHitCount() {
        return decrementHits.incrementAndGet();
    }

    @Override
    public long incrementDecrMissCount() {
        return decrementMisses.incrementAndGet();
    }

    @Override
    public long incrementTouchCount() {
        return touches.incrementAndGet();
    }

    @Override
    public RestCallCollector getRestCallCollector() {
        return restCallCollector;
    }

    @Override
    public void processRequest(TextCommand command) {
        startResponseThreadIfNotRunning();
        node.nodeEngine.getExecutionService().execute("hz:text", new CommandExecutor(command));
    }

    private void startResponseThreadIfNotRunning() {
        if (responseThreadRunnable == null) {
            synchronized (mutex) {
                if (responseThreadRunnable == null) {
                    responseThreadRunnable = new ResponseThreadRunnable();
                    String threadNamePrefix = createThreadName(hazelcast.getName(), "ascii.service.response");
                    Thread thread = new Thread(responseThreadRunnable, threadNamePrefix);
                    thread.start();
                }
            }
        }
    }

    @Override
    public Object get(String mapName, String key) {
        return hazelcast.getMap(mapName).get(key);
    }

    @Override
    public Map<String, Object> getAll(String mapName, Set<String> keys) {
        IMap<String, Object> map = hazelcast.getMap(mapName);
        return map.getAll(keys);
    }

    @Override
    public int getAdjustedTTLSeconds(int ttl) {
        if (ttl <= TextCommandConstants.getMonthSeconds()) {
            return ttl;
        } else {
            return ttl - (int) (TimeUnit.MILLISECONDS.toSeconds(Clock.currentTimeMillis()));
        }
    }

    @Override
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

    @Override
    public Object put(String mapName, String key, Object value) {
        return hazelcast.getMap(mapName).put(key, value);
    }

    @Override
    public Object put(String mapName, String key, Object value, int ttlSeconds) {
        return hazelcast.getMap(mapName).put(key, value, ttlSeconds, TimeUnit.SECONDS);
    }

    @Override
    public Object putIfAbsent(String mapName, String key, Object value, int ttlSeconds) {
        return hazelcast.getMap(mapName).putIfAbsent(key, value, ttlSeconds, TimeUnit.SECONDS);
    }

    @Override
    public Object replace(String mapName, String key, Object value) {
        return hazelcast.getMap(mapName).replace(key, value);
    }

    @Override
    public void lock(String mapName, String key) throws InterruptedException {
        if (!hazelcast.getMap(mapName).tryLock(key, 1, TimeUnit.MINUTES)) {
            throw new RuntimeException("Memcache client could not get the lock for map: "
                    + mapName + ", key: " + key + " in 1 minute");
        }
    }

    @Override
    public void unlock(String mapName, String key) {
        hazelcast.getMap(mapName).unlock(key);
    }

    @Override
    public void deleteAll(String mapName) {
        final IMap<Object, Object> map = hazelcast.getMap(mapName);
        map.clear();
    }

    @Override
    public Object delete(String mapName, String key) {
        return hazelcast.getMap(mapName).remove(key);
    }

    @Override
    public boolean offer(String queueName, Object value) {
        return hazelcast.getQueue(queueName).offer(value);
    }

    @Override
    public Object poll(String queueName, int seconds) {
        try {
            return hazelcast.getQueue(queueName).poll(seconds, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            currentThread().interrupt();
            return null;
        }
    }

    @Override
    public Object poll(String queueName) {
        return hazelcast.getQueue(queueName).poll();
    }

    @Override
    public int size(String queueName) {
        return hazelcast.getQueue(queueName).size();
    }

    @Override
    public void sendResponse(TextCommand textCommand) {
        textCommand.beforeSendResponse(this);
        if (!textCommand.shouldReply() || textCommand.getRequestId() == -1) {
            throw new RuntimeException("Shouldn't reply " + textCommand);
        }
        responseThreadRunnable.sendResponse(textCommand);
    }

    @Override
    public void stop() {
        final ResponseThreadRunnable rtr = responseThreadRunnable;
        if (rtr != null) {
            logger.info("Stopping text command service...");
            rtr.stop();
        }
    }

    @Override
    public String getInstanceName() {
        return hazelcast.getName();
    }

    class CommandExecutor implements Runnable {
        final TextCommand command;

        CommandExecutor(TextCommand command) {
            this.command = command;
        }

        @Override
        public void run() {
            try {
                TextCommandConstants.TextCommandType type = command.getType();
                TextCommandProcessor textCommandProcessor = textCommandProcessors[type.getValue()];
                textCommandProcessor.handle(command);
            } catch (Throwable e) {
                logger.warning(e);
            }
        }
    }

    private class ResponseThreadRunnable implements Runnable {
        private final BlockingQueue<TextCommand> blockingQueue = new ArrayBlockingQueue<TextCommand>(200);
        private final Object stopObject = new Object();

        @SuppressFBWarnings("RV_RETURN_VALUE_IGNORED_BAD_PRACTICE")
        public void sendResponse(TextCommand textCommand) {
            blockingQueue.offer(textCommand);
        }

        @Override
        public void run() {
            while (running) {
                try {
                    TextCommand textCommand = blockingQueue.take();
                    if (TextCommandConstants.TextCommandType.STOP == textCommand.getType()) {
                        synchronized (stopObject) {
                            stopObject.notify();
                        }
                    } else {
                        TextEncoder textWriteHandler = textCommand.getEncoder();
                        textWriteHandler.enqueue(textCommand);
                    }
                } catch (InterruptedException e) {
                    currentThread().interrupt();
                    return;
                } catch (OutOfMemoryError e) {
                    OutOfMemoryErrorDispatcher.onOutOfMemory(e);
                    throw e;
                } catch (Throwable t) {
                    logger.severe("Error while processing Memcache or Rest command.", t);
                }
            }
        }

        @SuppressFBWarnings("RV_RETURN_VALUE_IGNORED_BAD_PRACTICE")
        void stop() {
            running = false;
            synchronized (stopObject) {
                try {
                    blockingQueue.offer(new AbstractTextCommand(TextCommandConstants.TextCommandType.STOP) {
                        @Override
                        public boolean readFrom(ByteBuffer src) {
                            return true;
                        }

                        @Override
                        public boolean writeTo(ByteBuffer dst) {
                            return true;
                        }
                    });
                    //noinspection WaitNotInLoop
                    stopObject.wait(WAIT_TIME);
                } catch (Exception ignored) {
                    ignore(ignored);
                }
            }
        }
    }
}
