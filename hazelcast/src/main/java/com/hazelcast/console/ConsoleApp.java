/*
 * Copyright (c) 2008-2025, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.console;

import com.hazelcast.cluster.Member;
import com.hazelcast.collection.IList;
import com.hazelcast.collection.IQueue;
import com.hazelcast.collection.ISet;
import com.hazelcast.collection.ItemEvent;
import com.hazelcast.collection.ItemListener;
import com.hazelcast.config.Config;
import com.hazelcast.config.ExecutorConfig;
import com.hazelcast.core.DistributedObject;
import com.hazelcast.core.EntryEvent;
import com.hazelcast.core.EntryListener;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IExecutorService;
import com.hazelcast.cp.IAtomicLong;
import com.hazelcast.internal.util.Clock;
import com.hazelcast.internal.util.RuntimeAvailableProcessors;
import com.hazelcast.map.IMap;
import com.hazelcast.map.MapEvent;
import com.hazelcast.multimap.MultiMap;
import com.hazelcast.partition.Partition;
import com.hazelcast.topic.ITopic;
import com.hazelcast.topic.Message;
import com.hazelcast.topic.MessageListener;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.PrintStream;
import java.lang.management.ManagementFactory;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.StringTokenizer;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;

import static com.hazelcast.internal.util.MapUtil.createHashMap;
import static com.hazelcast.internal.util.StringUtil.equalsIgnoreCase;
import static com.hazelcast.internal.util.StringUtil.lowerCaseInternal;
import static com.hazelcast.memory.MemoryUnit.BYTES;
import static java.lang.String.format;
import static java.lang.Thread.currentThread;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.concurrent.TimeUnit.MILLISECONDS;

/**
 * Special thanks to Alexandre Vasseur for providing this very nice test application.
 */
@SuppressWarnings({"checkstyle:magicnumber", "ClassFanOutComplexity", "MethodCount"})
public class ConsoleApp implements EntryListener<Object, Object>, ItemListener<Object>, MessageListener<Object> {

    private static final String EXECUTOR_NAMESPACE = "Sample Executor";
    private static final int LOAD_EXECUTORS_COUNT = 16;
    private static final int ONE_HUNDRED = 100;

    private IQueue<Object> queue;
    private ITopic<Object> topic;
    private IMap<Object, Object> map;
    private MultiMap<Object, Object> multiMap;
    private ISet<Object> set;
    private IList<Object> list;
    private IAtomicLong atomicNumber;

    private String namespace = "default";
    private boolean silent;
    private boolean echo;

    private volatile boolean running;

    private final PrintStream outOrig;
    private final HazelcastInstance hazelcast;

    public ConsoleApp(HazelcastInstance hazelcast, PrintStream outOrig) {
        this.hazelcast = hazelcast;
        this.outOrig = outOrig;
    }

    public IQueue<Object> getQueue() {
        queue = hazelcast.getQueue(namespace);
        return queue;
    }

    public ITopic<Object> getTopic() {
        topic = hazelcast.getTopic(namespace);
        return topic;
    }

    public IMap<Object, Object> getMap() {
        map = hazelcast.getMap(namespace);
        return map;
    }

    public MultiMap<Object, Object> getMultiMap() {
        multiMap = hazelcast.getMultiMap(namespace);
        return multiMap;
    }

    public IAtomicLong getAtomicNumber() {
        atomicNumber = hazelcast.getCPSubsystem().getAtomicLong(namespace);
        return atomicNumber;
    }

    public ISet<Object> getSet() {
        set = hazelcast.getSet(namespace);
        return set;
    }

    public IList<Object> getList() {
        list = hazelcast.getList(namespace);
        return list;
    }

    public void stop() {
        running = false;
    }

    public void start() throws Exception {
        getMap().size();
        getList().size();
        getSet().size();
        getQueue().size();
        getTopic().getLocalTopicStats();
        getMultiMap().size();
        hazelcast.getExecutorService("default").getLocalExecutorStats();
        for (int i = 1; i <= LOAD_EXECUTORS_COUNT; i++) {
            hazelcast.getExecutorService(EXECUTOR_NAMESPACE + " " + i).getLocalExecutorStats();
        }

        try (LineReader lineReader = new DefaultLineReader()) {
            running = true;
            while (running) {
                print("hazelcast[" + namespace + "] > ");
                try {
                    final String command = lineReader.readLine();
                    handleCommand(command);
                } catch (Throwable e) {
                    e.printStackTrace();
                }
            }
        }
    }

    /**
     * Handles a command.
     */
    @SuppressWarnings({"checkstyle:cyclomaticcomplexity", "checkstyle:npathcomplexity", "checkstyle:methodlength"})
    public void handleCommand(String inputCommand) {
        String command = inputCommand;
        if (command == null) {
            return;
        }
        command = command.strip();
        if (command.isEmpty()) {
            return;
        }
        if (command.contains("__")) {
            namespace = command.split("__")[0];
            command = command.substring(command.indexOf("__") + 2);
        }
        if (echo) {
            handleEcho(command);
        }
        if (command.startsWith("//")) {
            return;
        }

        String first = command;
        int spaceIndex = command.indexOf(' ');
        String[] argsSplit = command.split(" ");
        String[] args = new String[argsSplit.length];
        for (int i = 0; i < argsSplit.length; i++) {
            args[i] = argsSplit[i].strip();
        }
        if (spaceIndex != -1) {
            first = args[0];
        }
        if (command.startsWith("help")) {
            handleHelp();
        } else if (first.startsWith("#") && first.length() > 1) {
            int repeat = Integer.parseInt(first.substring(1));
            long started = Clock.currentTimeMillis();
            for (int i = 0; i < repeat; i++) {
                handleCommand(command.substring(first.length()).replaceAll("\\$i", String.valueOf(i)));
            }
            long elapsedMilliSeconds = Clock.currentTimeMillis() - started;
            if (elapsedMilliSeconds > 0) {
                println(String.format("ops/s = %.2f", (double) repeat * 1000 / elapsedMilliSeconds));
            } else {
                println("Bingo, all the operations finished in no time!");
            }
        } else if (first.startsWith("&") && first.length() > 1) {
            final int fork = Integer.parseInt(first.substring(1));
            final String threadCommand = command.substring(first.length());
            ExecutorService pool = Executors.newFixedThreadPool(fork);
            for (int i = 0; i < fork; i++) {
                final int threadID = i;
                pool.submit(() -> {
                    String sanitizedCommand = threadCommand;
                    String[] threadArgs = sanitizedCommand.replaceAll("\\$t", String.valueOf(threadID)).strip().split(" ");
                    // TODO &t #4 m.putmany x k
                    if ("m.putmany".equals(threadArgs[0]) || "m.removemany".equals(threadArgs[0])) {
                        if (threadArgs.length < 4) {
                            sanitizedCommand += " " + Integer.parseInt(threadArgs[1]) * threadID;
                        }
                    }
                    handleCommand(sanitizedCommand);
                });
            }
            pool.shutdown();
            try {
                pool.awaitTermination(1, TimeUnit.HOURS);
            } catch (Exception e) {
                e.printStackTrace();
            }
        } else if (first.startsWith("@")) {
            handleAt(first);
        } else if (command.indexOf(';') != -1) {
            handleColon(command);
        } else if ("silent".equals(first)) {
            silent = Boolean.parseBoolean(args[1]);
        } else if (equalsIgnoreCase("shutdown", first)) {
            handleShutdown();
        } else if ("echo".equals(first)) {
            echo = Boolean.parseBoolean(args[1]);
            println("echo: " + echo);
        } else if ("ns".equals(first)) {
            handleNamespace(command.substring(first.length()).strip());
        } else if ("whoami".equals(first)) {
            handleWhoami();
        } else if ("who".equals(first)) {
            handleWho();
        } else if ("jvm".equals(first)) {
            handleJvm();
        } else if (first.contains("ock") && !first.contains(".")) {
            handleLock(args);
        } else if (first.contains(".size")) {
            handleSize(args);
        } else if (first.contains(".clear")) {
            handleClear(args);
        } else if (first.contains(".destroy")) {
            handleDestroy(args);
        } else if (first.contains(".iterator")) {
            handleIterator(args);
        } else if (first.contains(".contains")) {
            handleContains(args);
        } else if (first.contains(".stats")) {
            handStats(args);
        } else if ("t.publish".equals(first)) {
            handleTopicPublish(args);
        } else if ("q.offer".equals(first)) {
            handleQOffer(args);
        } else if ("q.take".equals(first)) {
            handleQTake();
        } else if ("q.poll".equals(first)) {
            handleQPoll(args);
        } else if ("q.peek".equals(first)) {
            handleQPeek();
        } else if ("q.capacity".equals(first)) {
            handleQCapacity();
        } else if ("q.offermany".equals(first)) {
            handleQOfferMany(args);
        } else if ("q.pollmany".equals(first)) {
            handleQPollMany(args);
        } else if ("s.add".equals(first)) {
            handleSetAdd(args);
        } else if ("s.remove".equals(first)) {
            handleSetRemove(args);
        } else if ("s.addmany".equals(first)) {
            handleSetAddMany(args);
        } else if ("s.removemany".equals(first)) {
            handleSetRemoveMany(args);
        } else if (first.equals("m.replace")) {
            handleMapReplace(args);
        } else if (equalsIgnoreCase(first, "m.putIfAbsent")) {
            handleMapPutIfAbsent(args);
        } else if (first.equals("m.putAsync")) {
            handleMapPutAsync(args);
        } else if (first.equals("m.getAsync")) {
            handleMapGetAsync(args);
        } else if (first.equals("m.put")) {
            handleMapPut(args);
        } else if (first.equals("m.get")) {
            handleMapGet(args);
        } else if (equalsIgnoreCase(first, "m.getMapEntry")) {
            handleMapGetMapEntry(args);
        } else if (first.equals("m.remove")) {
            handleMapRemove(args);
        } else if (first.equals("m.delete")) {
            handleMapDelete(args);
        } else if (first.equals("m.evict")) {
            handleMapEvict(args);
        } else if (first.equals("m.putmany") || equalsIgnoreCase(first, "m.putAll")) {
            handleMapPutMany(args);
        } else if (first.equals("m.getmany")) {
            handleMapGetMany(args);
        } else if (first.equals("m.removemany")) {
            handleMapRemoveMany(args);
        } else if (equalsIgnoreCase(command, "m.localKeys")) {
            handleMapLocalKeys();
        } else if (equalsIgnoreCase(command, "m.localSize")) {
            handleMapLocalSize();
        } else if (command.equals("m.keys")) {
            handleMapKeys();
        } else if (command.equals("m.values")) {
            handleMapValues();
        } else if (command.equals("m.entries")) {
            handleMapEntries();
        } else if (first.equals("m.lock")) {
            handleMapLock(args);
        } else if (equalsIgnoreCase(first, "m.tryLock")) {
            handleMapTryLock(args);
        } else if (first.equals("m.unlock")) {
            handleMapUnlock(args);
        } else if (first.contains(".addListener")) {
            handleAddListener(args);
        } else if (first.equals("m.removeMapListener")) {
            handleRemoveListener(args);
        } else if (first.equals("mm.put")) {
            handleMultiMapPut(args);
        } else if (first.equals("mm.get")) {
            handleMultiMapGet(args);
        } else if (first.equals("mm.remove")) {
            handleMultiMapRemove(args);
        } else if (command.equals("mm.keys")) {
            handleMultiMapKeys();
        } else if (command.equals("mm.values")) {
            handleMultiMapValues();
        } else if (command.equals("mm.entries")) {
            handleMultiMapEntries();
        } else if (first.equals("mm.lock")) {
            handleMultiMapLock(args);
        } else if (equalsIgnoreCase(first, "mm.tryLock")) {
            handleMultiMapTryLock(args);
        } else if (first.equals("mm.unlock")) {
            handleMultiMapUnlock(args);
        } else if (first.equals("l.add")) {
            handleListAdd(args);
        } else if (first.equals("l.set")) {
            handleListSet(args);
        } else if ("l.addmany".equals(first)) {
            handleListAddMany(args);
        } else if (first.equals("l.remove")) {
            handleListRemove(args);
        } else if (first.equals("l.contains")) {
            handleListContains(args);
        } else if ("a.get".equals(first)) {
            handleAtomicNumberGet();
        } else if ("a.set".equals(first)) {
            handleAtomicNumberSet(args);
        } else if ("a.inc".equals(first)) {
            handleAtomicNumberInc();
        } else if ("a.dec".equals(first)) {
            handleAtomicNumberDec();
        } else if (first.equals("execute")) {
            execute(args);
        } else if (first.equals("partitions")) {
            handlePartitions();
        } else if (equalsIgnoreCase(first, "executeOnKey")) {
            executeOnKey(args);
        } else if (equalsIgnoreCase(first, "executeOnMember")) {
            executeOnMember(args);
        } else if (equalsIgnoreCase(first, "executeOnMembers")) {
            executeOnMembers(args);
        } else if (equalsIgnoreCase(first, "instances")) {
            handleInstances();
        } else if (equalsIgnoreCase(first, "quit") || equalsIgnoreCase(first, "exit")) {
            handleExit();
        } else if (first.startsWith("e") && first.endsWith(".simulateLoad")) {
            handleExecutorSimulate(args);
        } else {
            println("type 'help' for help");
        }
    }

    protected void handleShutdown() {
        hazelcast.getLifecycleService().shutdown();
    }

    protected void handleExit() {
        System.exit(0);
    }

    private void handleExecutorSimulate(String[] args) {
        String first = args[0];
        int threadCount = Integer.parseInt(first.substring(1, first.indexOf(".")));
        if (threadCount < 1 || threadCount > 16) {
            throw new RuntimeException("threadCount can't be smaller than 1 or larger than 16");
        }

        int taskCount = Integer.parseInt(args[1]);
        int durationSec = Integer.parseInt(args[2]);

        long startMs = System.currentTimeMillis();

        IExecutorService executor = hazelcast.getExecutorService(EXECUTOR_NAMESPACE + " " + threadCount);
        List<Future<Object>> futures = new LinkedList<>();
        List<Member> members = new LinkedList<>(hazelcast.getCluster().getMembers());

        int totalThreadCount = hazelcast.getCluster().getMembers().size() * threadCount;

        int latchId = 0;
        for (int i = 0; i < taskCount; i++) {
            Member member = members.get(i % members.size());
            if (taskCount % totalThreadCount == 0) {
                latchId = taskCount / totalThreadCount;
                hazelcast.getCPSubsystem().getCountDownLatch("latch" + latchId).trySetCount(totalThreadCount);

            }
            Future<Object> f = executor.submitToMember(new SimulateLoadTask(durationSec, i + 1, "latch" + latchId), member);
            futures.add(f);
        }

        for (Future f : futures) {
            try {
                f.get();
            } catch (InterruptedException e) {
                currentThread().interrupt();
                e.printStackTrace();
            } catch (ExecutionException e) {
                e.printStackTrace();
            }
        }

        long durationMs = System.currentTimeMillis() - startMs;
        println(format("Executed %s tasks in %s ms", taskCount, durationMs));
    }

    private void handleColon(String command) {
        StringTokenizer st = new StringTokenizer(command, ";");
        while (st.hasMoreTokens()) {
            handleCommand(st.nextToken());
        }
    }

    private void handleAt(String first) {
        if (first.length() == 1) {
            println("usage: @<file-name>");
            return;
        }
        File f = new File(first.substring(1));
        println("Executing script file " + f.getAbsolutePath());
        if (f.exists()) {
            try (BufferedReader br = new BufferedReader(new FileReader(f, UTF_8))) {
                String l = br.readLine();
                while (l != null) {
                    handleCommand(l);
                    l = br.readLine();
                }
            } catch (IOException e) {
                e.printStackTrace();
            }
        } else {
            println("File not found! " + f.getAbsolutePath());
        }
    }

    private void handleEcho(String command) {
        String threadName = lowerCaseInternal(Thread.currentThread().getName());
        if (!threadName.contains("main")) {
            println(" [" + Thread.currentThread().getName() + "] " + command);
        } else {
            println(command);
        }
    }

    private void handleNamespace(String namespace) {
        if (!namespace.isEmpty()) {
            this.namespace = namespace;
        }
        println("namespace: " + namespace);
    }

    @SuppressFBWarnings("DM_GC")
    private void handleJvm() {
        System.gc();

        long max = Runtime.getRuntime().maxMemory();
        long total = Runtime.getRuntime().totalMemory();
        long free = Runtime.getRuntime().freeMemory();
        println("Memory max: " + BYTES.toMegaBytes(max) + "MB");
        println("Memory free: " + BYTES.toMegaBytes(free) + "MB " + (int) (free * ONE_HUNDRED / max) + "%");
        println("Used Memory:" + BYTES.toMegaBytes(total - free) + "MB");
        println("# procs: " + RuntimeAvailableProcessors.get());
        println("OS info: " + ManagementFactory.getOperatingSystemMXBean().getArch()
                + " " + ManagementFactory.getOperatingSystemMXBean().getName()
                + " " + ManagementFactory.getOperatingSystemMXBean().getVersion());
        println("JVM: " + ManagementFactory.getRuntimeMXBean().getVmVendor()
                + " " + ManagementFactory.getRuntimeMXBean().getVmName()
                + " " + ManagementFactory.getRuntimeMXBean().getVmVersion());
    }

    private void handleWhoami() {
        println(hazelcast.getCluster().getLocalMember());
    }

    private void handleWho() {
        StringBuilder sb = new StringBuilder("\n\nMembers [");
        final Collection<Member> members = hazelcast.getCluster().getMembers();
        sb.append(members != null ? members.size() : 0);
        sb.append("] {");
        if (members != null) {
            for (Member member : members) {
                sb.append("\n\t").append(member);
            }
        }
        sb.append("\n}\n");
        println(sb.toString());
    }

    private void handleAtomicNumberGet() {
        println(getAtomicNumber().get());
    }

    private void handleAtomicNumberSet(String[] args) {
        long v = 0;
        if (args.length > 1) {
            v = Long.parseLong(args[1]);
        }
        getAtomicNumber().set(v);
        println(getAtomicNumber().get());
    }

    private void handleAtomicNumberInc() {
        println(getAtomicNumber().incrementAndGet());
    }

    private void handleAtomicNumberDec() {
        println(getAtomicNumber().decrementAndGet());
    }

    protected void handlePartitions() {
        Set<Partition> partitions = hazelcast.getPartitionService().getPartitions();
        Map<Member, Integer> partitionCounts = new HashMap<>();
        for (Partition partition : partitions) {
            Member owner = partition.getOwner();
            if (owner != null) {
                Integer count = partitionCounts.get(owner);
                int newCount = 1;
                if (count != null) {
                    newCount = count + 1;
                }
                partitionCounts.put(owner, newCount);
            }
            println(partition);
        }
        Set<Map.Entry<Member, Integer>> entries = partitionCounts.entrySet();
        for (Map.Entry<Member, Integer> entry : entries) {
            println(entry.getKey() + ": " + entry.getValue());
        }
    }

    protected void handleInstances() {
        Collection<DistributedObject> distributedObjects = hazelcast.getDistributedObjects();
        for (DistributedObject distributedObject : distributedObjects) {
            println(distributedObject);
        }
    }

    // ==================== list ===================================

    protected void handleListContains(String[] args) {
        println(getList().contains(args[1]));
    }

    protected void handleListRemove(String[] args) {
        try {
            int index = Integer.parseInt(args[1]);
            println(getList().remove(index));
        } catch (NumberFormatException e) {
            println(getList().remove(args[1]));
        }
    }

    protected void handleListAdd(String[] args) {
        if (args.length == 3) {
            final int index = Integer.parseInt(args[1]);
            getList().add(index, args[2]);
            println("true");
        } else {
            println(getList().add(args[1]));
        }
    }

    protected void handleListSet(String[] args) {
        final int index = Integer.parseInt(args[1]);
        println(getList().set(index, args[2]));
    }

    protected void handleListAddMany(String[] args) {
        int count = 1;
        if (args.length > 1) {
            count = Integer.parseInt(args[1]);
        }
        int successCount = 0;
        long started = Clock.currentTimeMillis();
        for (int i = 0; i < count; i++) {
            boolean success = getList().add("obj" + i);
            if (success) {
                successCount++;
            }
        }
        long elapsedMillis = Clock.currentTimeMillis() - started;
        println("Added " + successCount + " objects.");
        if (elapsedMillis > 0) {
            println("size = " + list.size() + ", " + MILLISECONDS.toSeconds(successCount / elapsedMillis) + " evt/s");
        }
    }

    // ==================== map ===================================

    protected void handleMapPut(String[] args) {
        if (args.length == 1) {
            println("m.put requires a key and a value. You have not specified either.");
        } else if (args.length == 2) {
            println("m.put requires a key and a value. You have only specified the key " + args[1]);
        } else if (args.length > 3) {
            println("m.put takes two arguments, a key and a value. You have specified more than two arguments.");
        } else {
            println(getMap().put(args[1], args[2]));
        }
    }

    protected void handleMapPutAsync(String[] args) {
        try {
            println(getMap().putAsync(args[1], args[2]).toCompletableFuture().get());
        } catch (InterruptedException e) {
            currentThread().interrupt();
            e.printStackTrace();
        } catch (ExecutionException e) {
            e.printStackTrace();
        }
    }

    protected void handleMapPutIfAbsent(String[] args) {
        println(getMap().putIfAbsent(args[1], args[2]));
    }

    protected void handleMapReplace(String[] args) {
        println(getMap().replace(args[1], args[2]));
    }

    protected void handleMapGet(String[] args) {
        println(getMap().get(args[1]));
    }

    protected void handleMapGetAsync(String[] args) {
        try {
            println(getMap().getAsync(args[1]).toCompletableFuture().get());
        } catch (InterruptedException e) {
            currentThread().interrupt();
            e.printStackTrace();
        } catch (ExecutionException e) {
            e.printStackTrace();
        }
    }

    protected void handleMapGetMapEntry(String[] args) {
        println(getMap().getEntryView(args[1]));
    }

    protected void handleMapRemove(String[] args) {
        println(getMap().remove(args[1]));
    }

    protected void handleMapDelete(String[] args) {
        getMap().delete(args[1]);
        println("true");
    }

    protected void handleMapEvict(String[] args) {
        println(getMap().evict(args[1]));
    }

    protected void handleMapPutMany(String[] args) {
        int count = 1;
        if (args.length > 1) {
            count = Integer.parseInt(args[1]);
        }
        int b = ONE_HUNDRED;
        byte[] value = new byte[b];
        if (args.length > 2) {
            b = Integer.parseInt(args[2]);
            value = new byte[b];
        }
        int start = getMap().size();
        if (args.length > 3) {
            start = Integer.parseInt(args[3]);
        }
        Map<String, byte[]> theMap = createHashMap(count);
        for (int i = 0; i < count; i++) {
            theMap.put("key" + (start + i), value);
        }
        long started = Clock.currentTimeMillis();
        getMap().putAll(theMap);
        long elapsedMillis = Clock.currentTimeMillis() - started;
        if (elapsedMillis > 0) {
            long addedKiloBytes = count * BYTES.toKiloBytes(b);
            println("size = " + getMap().size() + ", " + MILLISECONDS.toSeconds(count / elapsedMillis) + " evt/s, "
                    + MILLISECONDS.toSeconds(addedKiloBytes * 8 / elapsedMillis) + " KBit/s, "
                    + addedKiloBytes + " KB added");
        }
    }

    protected void handleMapGetMany(String[] args) {
        int count = 1;
        if (args.length > 1) {
            count = Integer.parseInt(args[1]);
        }
        for (int i = 0; i < count; i++) {
            println(getMap().get("key" + i));
        }
    }

    protected void handleMapRemoveMany(String[] args) {
        int count = 1;
        if (args.length > 1) {
            count = Integer.parseInt(args[1]);
        }
        int start = 0;
        if (args.length > 2) {
            start = Integer.parseInt(args[2]);
        }
        long started = Clock.currentTimeMillis();
        for (int i = 0; i < count; i++) {
            getMap().remove("key" + (start + i));
        }
        long elapsedMillis = Clock.currentTimeMillis() - started;
        if (elapsedMillis > 0) {
            println("size = " + getMap().size() + ", " + MILLISECONDS.toSeconds(count / elapsedMillis) + " evt/s");
        }
    }

    protected void handleMapLock(String[] args) {
        getMap().lock(args[1]);
        println("true");
    }

    protected void handleMapTryLock(String[] args) {
        String key = args[1];
        long time = (args.length > 2) ? Long.parseLong(args[2]) : 0;
        boolean locked;
        if (time == 0) {
            locked = getMap().tryLock(key);
        } else {
            try {
                locked = getMap().tryLock(key, time, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                currentThread().interrupt();
                locked = false;
            }
        }
        println(locked);
    }

    protected void handleMapUnlock(String[] args) {
        getMap().unlock(args[1]);
        println("true");
    }

    protected void handleMapLocalKeys() {
        Set<Object> keySet = getMap().localKeySet();
        Iterator<Object> it = keySet.iterator();
        int count = 0;
        while (it.hasNext()) {
            count++;
            println(it.next());
        }
        println("Total " + count);
    }

    protected void handleMapLocalSize() {
        println("Local Size = " + getMap().localKeySet().size());
    }

    protected void handleMapKeys() {
        Set<Object> keySet = getMap().keySet();
        Iterator<Object> it = keySet.iterator();
        int count = 0;
        while (it.hasNext()) {
            count++;
            println(it.next());
        }
        println("Total " + count);
    }

    protected void handleMapEntries() {
        Set<Map.Entry<Object, Object>> entrySet = getMap().entrySet();
        Iterator<Map.Entry<Object, Object>> it = entrySet.iterator();
        int count = 0;
        while (it.hasNext()) {
            count++;
            Map.Entry<Object, Object> entry = it.next();
            println(entry.getKey() + ": " + entry.getValue());
        }
        println("Total " + count);
    }

    protected void handleMapValues() {
        Collection<Object> values = getMap().values();
        Iterator<Object> it = values.iterator();
        int count = 0;
        while (it.hasNext()) {
            count++;
            println(it.next());
        }
        println("Total " + count);
    }

    // ==================== multimap ===================================

    protected void handleMultiMapPut(String[] args) {
        println(getMultiMap().put(args[1], args[2]));
    }

    protected void handleMultiMapGet(String[] args) {
        println(getMultiMap().get(args[1]));
    }

    protected void handleMultiMapRemove(String[] args) {
        println(getMultiMap().remove(args[1]));
    }

    protected void handleMultiMapKeys() {
        Set<Object> keySet = getMultiMap().keySet();
        Iterator<Object> it = keySet.iterator();
        int count = 0;
        while (it.hasNext()) {
            count++;
            println(it.next());
        }
        println("Total " + count);
    }

    protected void handleMultiMapEntries() {
        Set<Map.Entry<Object, Object>> entrySet = getMultiMap().entrySet();
        Iterator<Map.Entry<Object, Object>> it = entrySet.iterator();
        int count = 0;
        while (it.hasNext()) {
            count++;
            Map.Entry<Object, Object> entry = it.next();
            println(entry.getKey() + ": " + entry.getValue());
        }
        println("Total " + count);
    }

    protected void handleMultiMapValues() {
        Collection<Object> values = getMultiMap().values();
        Iterator<Object> it = values.iterator();
        int count = 0;
        while (it.hasNext()) {
            count++;
            println(it.next());
        }
        println("Total " + count);
    }

    protected void handleMultiMapLock(String[] args) {
        getMultiMap().lock(args[1]);
        println("true");
    }

    protected void handleMultiMapTryLock(String[] args) {
        String key = args[1];
        long time = (args.length > 2) ? Long.parseLong(args[2]) : 0;
        boolean locked;
        if (time == 0) {
            locked = getMultiMap().tryLock(key);
        } else {
            try {
                locked = getMultiMap().tryLock(key, time, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                currentThread().interrupt();
                locked = false;
            }
        }
        println(locked);
    }

    protected void handleMultiMapUnlock(String[] args) {
        getMultiMap().unlock(args[1]);
        println("true");
    }

    // =======================================================

    private void handStats(String[] args) {
        String iteratorStr = args[0];
        if (iteratorStr.startsWith("m.")) {
            println(getMap().getLocalMapStats());
        } else if (iteratorStr.startsWith("mm.")) {
            println(getMultiMap().getLocalMultiMapStats());
        } else if (iteratorStr.startsWith("q.")) {
            println(getQueue().getLocalQueueStats());
        }
    }

    // squid:S2222 suppression avoids sonar analysis bug regarding already known lock release issue
    @SuppressWarnings({"LockAcquiredButNotSafelyReleased", "squid:S2222"})
    protected void handleLock(String[] args) {
        String lockStr = args[0];
        String key = args[1];
        Lock lock = hazelcast.getCPSubsystem().getLock(key);
        if (equalsIgnoreCase(lockStr, "lock")) {
            lock.lock();
            println("true");
        } else if (equalsIgnoreCase(lockStr, "unlock")) {
            lock.unlock();
            println("true");
        } else if (equalsIgnoreCase(lockStr, "trylock")) {
            String timeout = args.length > 2 ? args[2] : null;
            if (timeout == null) {
                println(lock.tryLock());
            } else {
                long time = Long.parseLong(timeout);
                try {
                    println(lock.tryLock(time, TimeUnit.SECONDS));
                } catch (InterruptedException e) {
                    currentThread().interrupt();
                    e.printStackTrace();
                }
            }
        }
    }

    protected void handleAddListener(String[] args) {
        String first = args[0];
        if (first.startsWith("s.")) {
            getSet().addItemListener(this, true);
        } else if (first.startsWith("m.")) {
            if (args.length > 1) {
                getMap().addEntryListener(this, args[1], true);
            } else {
                getMap().addEntryListener(this, true);
            }
        } else if (first.startsWith("mm.")) {
            if (args.length > 1) {
                getMultiMap().addEntryListener(this, args[1], true);
            } else {
                getMultiMap().addEntryListener(this, true);
            }
        } else if (first.startsWith("q.")) {
            getQueue().addItemListener(this, true);
        } else if (first.startsWith("t.")) {
            getTopic().addMessageListener(this);
        } else if (first.startsWith("l.")) {
            getList().addItemListener(this, true);
        }
    }

    protected void handleRemoveListener(String[] args) {
//        String first = args[0];
//        if (first.startsWith("s.")) {
//            getSet().removeItemListener(this);
//        } else if (first.startsWith("m.")) {
//            if (args.length > 1) {
//                // todo revise here
//                getMap().removeEntryListener(args[1]);
//            } else {
//                getMap().removeEntryListener(args[0]);
//            }
//        } else if (first.startsWith("q.")) {
//            getQueue().removeItemListener(this);
//        } else if (first.startsWith("t.")) {
//            getTopic().removeMessageListener(this);
//        } else if (first.startsWith("l.")) {
//            getList().removeItemListener(this);
//        }
    }

    protected void handleSetAdd(String[] args) {
        println(getSet().add(args[1]));
    }

    protected void handleSetRemove(String[] args) {
        println(getSet().remove(args[1]));
    }

    protected void handleSetAddMany(String[] args) {
        int count = 1;
        if (args.length > 1) {
            count = Integer.parseInt(args[1]);
        }
        int successCount = 0;
        long started = Clock.currentTimeMillis();
        for (int i = 0; i < count; i++) {
            boolean success = getSet().add("obj" + i);
            if (success) {
                successCount++;
            }
        }
        long elapsedMillis = Clock.currentTimeMillis() - started;
        println("Added " + successCount + " objects.");
        if (elapsedMillis > 0) {
            println("size = " + getSet().size() + ", " + MILLISECONDS.toSeconds(successCount / elapsedMillis) + " evt/s");
        }
    }

    protected void handleSetRemoveMany(String[] args) {
        int count = 1;
        if (args.length > 1) {
            count = Integer.parseInt(args[1]);
        }
        int successCount = 0;
        long started = Clock.currentTimeMillis();
        for (int i = 0; i < count; i++) {
            boolean success = getSet().remove("obj" + i);
            if (success) {
                successCount++;
            }
        }
        long elapsedMillis = Clock.currentTimeMillis() - started;
        println("Removed " + successCount + " objects.");
        if (elapsedMillis > 0) {
            println("size = " + getSet().size() + ", " + MILLISECONDS.toSeconds(successCount / elapsedMillis) + " evt/s");
        }
    }

    protected void handleIterator(String[] args) {
        Iterator<Object> it = null;
        String iteratorStr = args[0];
        if (iteratorStr.startsWith("s.")) {
            it = getSet().iterator();
        } else if (iteratorStr.startsWith("m.")) {
            it = getMap().keySet().iterator();
        } else if (iteratorStr.startsWith("mm.")) {
            it = getMultiMap().keySet().iterator();
        } else if (iteratorStr.startsWith("q.")) {
            it = getQueue().iterator();
        } else if (iteratorStr.startsWith("l.")) {
            it = getList().iterator();
        }
        if (it != null) {
            boolean remove = false;
            if (args.length > 1) {
                String removeStr = args[1];
                remove = removeStr.equals("remove");
            }
            int count = 1;
            while (it.hasNext()) {
                print(count++ + " " + it.next());
                if (remove) {
                    it.remove();
                    print(" removed");
                }
                println("");
            }
        }
    }

    protected void handleContains(String[] args) {
        String iteratorStr = args[0];
        boolean key = lowerCaseInternal(iteratorStr).endsWith("key");
        String data = args[1];
        boolean result = false;
        if (iteratorStr.startsWith("s.")) {
            result = getSet().contains(data);
        } else if (iteratorStr.startsWith("m.")) {
            result = (key) ? getMap().containsKey(data) : getMap().containsValue(data);
        } else if (iteratorStr.startsWith("mmm.")) {
            result = (key) ? getMultiMap().containsKey(data) : getMultiMap().containsValue(data);
        } else if (iteratorStr.startsWith("q.")) {
            result = getQueue().contains(data);
        } else if (iteratorStr.startsWith("l.")) {
            result = getList().contains(data);
        }
        println("Contains: " + result);
    }

    protected void handleSize(String[] args) {
        int size = 0;
        String iteratorStr = args[0];
        if (iteratorStr.startsWith("s.")) {
            size = getSet().size();
        } else if (iteratorStr.startsWith("m.")) {
            size = getMap().size();
        } else if (iteratorStr.startsWith("mm.")) {
            size = getMultiMap().size();
        } else if (iteratorStr.startsWith("q.")) {
            size = getQueue().size();
        } else if (iteratorStr.startsWith("l.")) {
            size = getList().size();
        }
        println("Size: " + size);
    }

    protected void handleClear(String[] args) {
        String iteratorStr = args[0];
        if (iteratorStr.startsWith("s.")) {
            getSet().clear();
        } else if (iteratorStr.startsWith("m.")) {
            getMap().clear();
        } else if (iteratorStr.startsWith("mm.")) {
            getMultiMap().clear();
        } else if (iteratorStr.startsWith("q.")) {
            getQueue().clear();
        } else if (iteratorStr.startsWith("l.")) {
            getList().clear();
        }
        println("Cleared all.");
    }

    protected void handleDestroy(String[] args) {
        String iteratorStr = args[0];
        if (iteratorStr.startsWith("s.")) {
            getSet().destroy();
        } else if (iteratorStr.startsWith("m.")) {
            getMap().destroy();
        } else if (iteratorStr.startsWith("mm.")) {
            getMultiMap().destroy();
        } else if (iteratorStr.startsWith("q.")) {
            getQueue().destroy();
        } else if (iteratorStr.startsWith("l.")) {
            getList().destroy();
        } else if (iteratorStr.startsWith("t.")) {
            getTopic().destroy();
        }
        println("Destroyed!");
    }

    protected void handleQOffer(String[] args) {
        long timeout = 0;
        if (args.length > 2) {
            timeout = Long.parseLong(args[2]);
        }
        try {
            boolean offered = getQueue().offer(args[1], timeout, TimeUnit.SECONDS);
            println(offered);
        } catch (InterruptedException e) {
            currentThread().interrupt();
            e.printStackTrace();
        }
    }

    protected void handleQTake() {
        try {
            println(getQueue().take());
        } catch (InterruptedException e) {
            currentThread().interrupt();
            e.printStackTrace();
        }
    }

    protected void handleQPoll(String[] args) {
        long timeout = 0;
        if (args.length > 1) {
            timeout = Long.parseLong(args[1]);
        }
        try {
            println(getQueue().poll(timeout, TimeUnit.SECONDS));
        } catch (InterruptedException e) {
            currentThread().interrupt();
            e.printStackTrace();
        }
    }

    protected void handleTopicPublish(String[] args) {
        getTopic().publish(args[1]);
    }

    protected void handleQOfferMany(String[] args) {
        int count = 1;
        if (args.length > 1) {
            count = Integer.parseInt(args[1]);
        }
        Object value = null;
        if (args.length > 2) {
            value = new byte[Integer.parseInt(args[2])];
        }
        long started = Clock.currentTimeMillis();
        for (int i = 0; i < count; i++) {
            if (value == null) {
                getQueue().offer("obj");
            } else {
                getQueue().offer(value);
            }
        }
        long elapsedMillis = Clock.currentTimeMillis() - started;
        print("size = " + getQueue().size() + ", " + MILLISECONDS.toSeconds(count / elapsedMillis) + " evt/s");
        if (value == null) {
            println("");
        } else if (elapsedMillis > 0) {
            int b = Integer.parseInt(args[2]);
            long addedKiloBytes = count * BYTES.toKiloBytes(b);
            println(", " + MILLISECONDS.toSeconds(addedKiloBytes * 8 / elapsedMillis) + " KBit/s, "
                    + addedKiloBytes + " KB added");
        }
    }

    protected void handleQPollMany(String[] args) {
        int count = 1;
        if (args.length > 1) {
            count = Integer.parseInt(args[1]);
        }
        int c = 1;
        for (int i = 0; i < count; i++) {
            Object obj = getQueue().poll();
            if (obj instanceof byte[] bytes) {
                println(c++ + " " + bytes.length);
            } else {
                println(c++ + " " + obj);
            }
        }
    }

    protected void handleQPeek() {
        println(getQueue().peek());
    }

    protected void handleQCapacity() {
        println(getQueue().remainingCapacity());
    }

    private void execute(String[] args) {
        // execute <echo-string>
        doExecute(false, false, args);
    }

    private void executeOnKey(String[] args) {
        // executeOnKey <echo-string> <key>
        doExecute(true, false, args);
    }

    private void executeOnMember(String[] args) {
        // executeOnMember <echo-string> <memberIndex>
        doExecute(false, true, args);
    }

    private void doExecute(boolean onKey, boolean onMember, String[] args) {
        // executeOnKey <echo-string> <key>
        try {
            IExecutorService executorService = hazelcast.getExecutorService("default");
            Echo callable = new Echo(args[1]);
            Future<String> future;
            if (onKey) {
                String key = args[2];
                future = executorService.submitToKeyOwner(callable, key);
            } else if (onMember) {
                int memberIndex = Integer.parseInt(args[2]);
                List<Member> members = new LinkedList<>(hazelcast.getCluster().getMembers());
                if (memberIndex >= members.size()) {
                    throw new IndexOutOfBoundsException("Member index: " + memberIndex + " must be smaller than "
                            + members.size());
                }
                Member member = members.get(memberIndex);
                future = executorService.submitToMember(callable, member);
            } else {
                future = executorService.submit(callable);
            }
            println("Result: " + future.get());
        } catch (InterruptedException e) {
            currentThread().interrupt();
            e.printStackTrace();
        } catch (ExecutionException e) {
            e.printStackTrace();
        }
    }

    private void executeOnMembers(String[] args) {
        // executeOnMembers <echo-string>
        try {
            IExecutorService executorService = hazelcast.getExecutorService("default");
            Echo task = new Echo(args[1]);
            Map<Member, Future<String>> results = executorService.submitToAllMembers(task);

            for (Future<String> f : results.values()) {
                println(f.get());
            }
        } catch (InterruptedException e) {
            currentThread().interrupt();
            e.printStackTrace();
        } catch (ExecutionException e) {
            e.printStackTrace();
        }
    }

    @Override
    public void entryAdded(EntryEvent<Object, Object> event) {
        println(event);
    }

    @Override
    public void entryRemoved(EntryEvent<Object, Object> event) {
        println(event);
    }

    @Override
    public void entryUpdated(EntryEvent<Object, Object> event) {
        println(event);
    }

    @Override
    public void entryEvicted(EntryEvent<Object, Object> event) {
        println(event);
    }

    @Override
    public void entryExpired(EntryEvent<Object, Object> event) {
        println(event);
    }

    @Override
    public void mapEvicted(MapEvent event) {
        println(event);
    }

    @Override
    public void mapCleared(MapEvent event) {
        println(event);
    }

    @Override
    public void itemAdded(ItemEvent itemEvent) {
        println("Item added = " + itemEvent.getItem());
    }

    @Override
    public void itemRemoved(ItemEvent itemEvent) {
        println("Item removed = " + itemEvent.getItem());
    }

    @Override
    public void onMessage(Message msg) {
        println("Topic received = " + msg.getMessageObject());
    }

    /**
     * Handled the help command
     */
    private void handleHelp() {
        boolean silentBefore = silent;
        silent = false;
        println("Commands:");

        printGeneralCommands();
        printQueueCommands();
        printSetCommands();
        printLockCommands();
        printMapCommands();
        printMulitiMapCommands();
        printListCommands();
        printAtomicLongCommands();
        printExecutorServiceCommands();

        silent = silentBefore;
    }

    private void printGeneralCommands() {
        println("-- General commands");
        println("echo true|false                      //turns on/off echo of commands (default false)");
        println("silent true|false                    //turns on/off silent of command output (default false)");
        println("#<number> <command>                  //repeats <number> time <command>, replace $i in <command> with current "
                + "iteration (0..<number-1>)");
        println("&<number> <command>                  //forks <number> threads to execute <command>, "
                + "replace $t in <command> with current thread number (0..<number-1>");
        println("     When using #x or &x, is is advised to use silent true as well.");
        println("     When using &x with m.putmany and m.removemany, each thread will get a different share of keys unless a "
                + "start key index is specified");
        println("jvm                                  //displays info about the runtime");
        println("who                                  //displays info about the cluster");
        println("whoami                               //displays info about this cluster member");
        println("ns <string>                          //switch the namespace for using the distributed data structure name "
                + " <string> (e.g. queue/map/set/list name; defaults to \"default\")");
        println("@<file>                              //executes the given <file> script. Use '//' for comments in the script");
        println("");
    }

    private void printQueueCommands() {
        println("-- Queue commands");
        println("q.offer <string>                     //adds a string object to the queue");
        println("q.poll                               //takes an object from the queue");
        println("q.offermany <number> [<size>]        //adds indicated number of string objects to the queue ('obj<i>' or "
                + "byte[<size>]) ");
        println("q.pollmany <number>                  //takes indicated number of objects from the queue");
        println("q.iterator [remove]                  //iterates the queue, remove if specified");
        println("q.size                               //size of the queue");
        println("q.clear                              //clears the queue");
        println("");
    }

    private void printSetCommands() {
        println("-- Set commands");
        println("s.add <string>                       //adds a string object to the set");
        println("s.remove <string>                    //removes the string object from the set");
        println("s.addmany <number>                   //adds indicated number of string objects to the set ('obj<i>')");
        println("s.removemany <number>                //takes indicated number of objects from the set");
        println("s.iterator [remove]                  //iterates the set, removes if specified");
        println("s.size                               //size of the set");
        println("s.clear                              //clears the set");
        println("");
    }

    private void printLockCommands() {
        println("-- Lock commands");
        println("lock <key>                           //same as Hazelcast.getCPSubsystem().getLock(key).lock()");
        println("tryLock <key>                        //same as Hazelcast.getCPSubsystem().getLock(key).tryLock()");
        println("tryLock <key> <time>                 //same as tryLock <key> with timeout in seconds");
        println("unlock <key>                         //same as Hazelcast.getCPSubsystem().getLock(key).unlock()");
        println("");
    }

    private void printMapCommands() {
        println("-- Map commands");
        println("m.put <key> <value>                  //puts an entry to the map");
        println("m.remove <key>                       //removes the entry of given key from the map");
        println("m.get <key>                          //returns the value of given key from the map");
        println("m.putmany <number> [<size>] [<index>]//puts indicated number of entries to the map ('key<i>':byte[<size>], "
                + "<index>+(0..<number>)");
        println("m.removemany <number> [<index>]      //removes indicated number of entries from the map ('key<i>', "
                + "<index>+(0..<number>)");
        println("     When using &x with m.putmany and m.removemany, each thread will get a different share of keys unless a "
                + "start key <index> is specified");
        println("m.keys                               //iterates the keys of the map");
        println("m.values                             //iterates the values of the map");
        println("m.entries                            //iterates the entries of the map");
        println("m.iterator [remove]                  //iterates the keys of the map, remove if specified");
        println("m.size                               //size of the map");
        println("m.localSize                          //local size of the map");
        println("m.clear                              //clears the map");
        println("m.destroy                            //destroys the map");
        println("m.lock <key>                         //locks the key");
        println("m.tryLock <key>                      //tries to lock the key and returns immediately");
        println("m.tryLock <key> <time>               //tries to lock the key within given seconds");
        println("m.unlock <key>                       //unlocks the key");
        println("m.stats                              //shows the local stats of the map");
        println("");
    }

    private void printMulitiMapCommands() {
        println("-- MultiMap commands");
        println("mm.put <key> <value>                  //puts an entry to the multimap");
        println("mm.get <key>                          //returns the value of given key from the multimap");
        println("mm.remove <key>                       //removes the entry of given key from the multimap");
        println("mm.size                               //size of the multimap");
        println("mm.clear                              //clears the multimap");
        println("mm.destroy                            //destroys the multimap");
        println("mm.iterator [remove]                  //iterates the keys of the multimap, remove if specified");
        println("mm.keys                               //iterates the keys of the multimap");
        println("mm.values                             //iterates the values of the multimap");
        println("mm.entries                            //iterates the entries of the multimap");
        println("mm.lock <key>                         //locks the key");
        println("mm.tryLock <key>                      //tries to lock the key and returns immediately");
        println("mm.tryLock <key> <time>               //tries to lock the key within given seconds");
        println("mm.unlock <key>                       //unlocks the key");
        println("mm.stats                              //shows the local stats of the multimap");
        println("");
    }

    private void printExecutorServiceCommands() {
        println("-- Executor Service commands:");
        println("execute <echo-input>                                     //executes an echo task on random member");
        println("executeOnKey <echo-input> <key>                          //executes an echo task on the member that owns "
                + "the given key");
        println("executeOnMember <echo-input> <memberIndex>               //executes an echo task on the member "
                + "with given index");
        println("executeOnMembers <echo-input>                            //executes an echo task on all of the members");
        println("e<threadcount>.simulateLoad <task-count> <delaySeconds>  //simulates load on executor with given number "
                + "of thread (e1..e16)");
        println("");
    }

    private void printAtomicLongCommands() {
        println("-- IAtomicLong commands:");
        println("a.get                                 //returns the value of the atomic long");
        println("a.set <long>                          //sets a value to the atomic long");
        println("a.inc                                 //increments the value of the atomic long by one");
        println("a.dec                                 //decrements the value of the atomic long by one");
        println("");
    }

    private void printListCommands() {
        println("-- List commands:");
        println("l.add <string>                        //adds a string object to the list");
        println("l.add <index> <string>                //adds a string object as an item with given index in the list");
        println("l.contains <string>                   //checks if the list contains a string object");
        println("l.remove <string>                     //removes a string object from the list");
        println("l.remove <index>                      //removes the item with given index from the list");
        println("l.set <index> <string>                //sets a string object to the item with given index in the list");
        println("l.iterator [remove]                   //iterates the list, remove if specified");
        println("l.size                                //size of the list");
        println("l.clear                               //clears the list");
        println("");
    }

    public void println(Object obj) {
        if (!silent) {
            outOrig.println(obj);
        }
    }

    public void print(Object obj) {
        if (!silent) {
            outOrig.print(obj);
        }
    }

    protected boolean isRunning() {
        return running;
    }

    protected static ConsoleApp create() {
        Config config = Config.load();
        for (int i = 1; i <= LOAD_EXECUTORS_COUNT; i++) {
            config.addExecutorConfig(new ExecutorConfig(EXECUTOR_NAMESPACE + " " + i).setPoolSize(i));
        }

        HazelcastInstance instance = Hazelcast.newHazelcastInstance(config);
        return new ConsoleApp(instance, System.out);
    }

    /**
     * Starts the test application.
     * <p>
     * It loads the Hazelcast member configuration using the resolution logic as described in
     * {@link com.hazelcast.core.Hazelcast#newHazelcastInstance()}.
     *
     * @throws Exception in case of any exceptional case
     */
    public static void main(String[] args) throws Exception {
        create().start();
    }
}
