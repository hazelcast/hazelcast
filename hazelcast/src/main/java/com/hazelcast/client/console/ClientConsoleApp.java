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

package com.hazelcast.client.console;

import com.hazelcast.client.HazelcastClient;
import com.hazelcast.client.impl.clientside.HazelcastClientInstanceImpl;
import com.hazelcast.client.impl.management.MCClusterMetadata;
import com.hazelcast.client.impl.spi.ClientClusterService;
import com.hazelcast.cluster.Cluster;
import com.hazelcast.cluster.Member;
import com.hazelcast.collection.IList;
import com.hazelcast.collection.IQueue;
import com.hazelcast.collection.ISet;
import com.hazelcast.collection.ItemEvent;
import com.hazelcast.collection.ItemListener;
import com.hazelcast.console.Echo;
import com.hazelcast.console.SimulateLoadTask;
import com.hazelcast.core.DistributedObject;
import com.hazelcast.core.EntryEvent;
import com.hazelcast.core.EntryListener;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IExecutorService;
import com.hazelcast.cp.IAtomicLong;
import com.hazelcast.internal.nio.IOUtil;
import com.hazelcast.internal.util.Clock;
import com.hazelcast.internal.util.FutureUtil;
import com.hazelcast.internal.util.RuntimeAvailableProcessors;
import com.hazelcast.map.IMap;
import com.hazelcast.map.MapEvent;
import com.hazelcast.multimap.MultiMap;
import com.hazelcast.partition.Partition;
import com.hazelcast.topic.ITopic;
import com.hazelcast.topic.Message;
import com.hazelcast.topic.MessageListener;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import org.jline.reader.EndOfFileException;
import org.jline.reader.History;
import org.jline.reader.LineReader;
import org.jline.reader.LineReaderBuilder;
import org.jline.reader.UserInterruptException;
import org.jline.terminal.Terminal;
import org.jline.terminal.TerminalBuilder;
import org.jline.utils.AttributedStringBuilder;
import org.jline.utils.AttributedStyle;
import org.jline.utils.InfoCmp;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOError;
import java.io.IOException;
import java.io.PrintWriter;
import java.lang.management.ManagementFactory;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.ListIterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.StringTokenizer;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;

import static com.hazelcast.client.console.HazelcastCommandLine.getClusterMetadata;
import static com.hazelcast.client.console.HazelcastCommandLine.getHazelcastClientInstanceImpl;
import static com.hazelcast.internal.util.StringUtil.equalsIgnoreCase;
import static com.hazelcast.internal.util.StringUtil.lowerCaseInternal;
import static java.lang.String.format;
import static java.lang.Thread.currentThread;
import static org.jline.utils.AttributedStyle.BLUE;
import static org.jline.utils.AttributedStyle.BRIGHT;

/**
 * A demo application to demonstrate a Hazelcast client. This is probably NOT something you want to use in production.
 */
@SuppressWarnings({"WeakerAccess", "unused", "checkstyle:ClassFanOutComplexity"})
public class ClientConsoleApp implements EntryListener, ItemListener, MessageListener {

    private static final int ONE_KB = 1024;
    private static final int ONE_THOUSAND = 1000;
    private static final int ONE_HUNDRED = 100;
    private static final int ONE_HOUR = 3600;

    private static final int MAX_THREAD_COUNT = 16;
    private static final int HUNDRED_CONSTANT = 100;
    private static final int BYTE_TO_BIT = 8;
    private static final int LENGTH_BORDER = 4;

    private static final int COLOR = BLUE | BRIGHT;

    private String namespace = "default";
    private String executorNamespace = "Sample Executor";

    private boolean silent;
    private boolean echo;

    private final LineReader lineReader;
    private final PrintWriter writer;
    private final HazelcastInstance client;

    private volatile boolean running;

    public ClientConsoleApp(@Nonnull HazelcastInstance client) {
        this(client, null);
    }

    public ClientConsoleApp(@Nonnull HazelcastInstance client, @Nullable PrintWriter writer) {
        this.client = client;

        Terminal terminal = createTerminal();

        lineReader = LineReaderBuilder.builder()
                // Check whether a real or dumb terminal. Dumb terminal enters an endless loop.
                // see issue: https://github.com/jline/jline3/issues/751
                .variable(LineReader.SECONDARY_PROMPT_PATTERN, isRealTerminal(terminal)
                        ? new AttributedStringBuilder().style(AttributedStyle.BOLD.foreground(COLOR))
                        .append("%M%P > ").toAnsi() : "")
                .variable(LineReader.INDENTATION, 2)
                .option(LineReader.Option.DISABLE_EVENT_EXPANSION, true)
                .appName("hazelcast-console-app")
                .terminal(terminal)
                .build();

        if (writer == null) {
            this.writer = lineReader.getTerminal().writer();
        } else {
            this.writer = writer;
        }
    }

    private Terminal createTerminal() {
        try {
            return TerminalBuilder.terminal();
        } catch (IOException e) {
            throw new IOError(e);
        }
    }

    private static boolean isRealTerminal(Terminal terminal) {
        return !Terminal.TYPE_DUMB.equals(terminal.getType())
                && !Terminal.TYPE_DUMB_COLOR.equals(terminal.getType());
    }

    public IQueue<Object> getQueue() {
        return client.getQueue(namespace);
    }

    public ITopic<Object> getTopic() {

        return client.getTopic(namespace);
    }

    public IMap<Object, Object> getMap() {
        return client.getMap(namespace);
    }

    public MultiMap<Object, Object> getMultiMap() {
        return client.getMultiMap(namespace);
    }

    public IAtomicLong getAtomicNumber() {
        return client.getCPSubsystem().getAtomicLong(namespace);
    }

    public ISet<Object> getSet() {
        return client.getSet(namespace);
    }

    public IList<Object> getList() {
        return client.getList(namespace);
    }

    public void stop() {
        running = false;
    }

    public void start() {
        try {
            println(startPrompt(client));
            writer.flush();
            running = true;
            while (running) {
                try {
                    final String command = lineReader.readLine(
                            new AttributedStringBuilder().style(AttributedStyle.DEFAULT.foreground(COLOR))
                                    .append("hazelcast[")
                                    .append(namespace)
                                    .append("] > ").toAnsi());
                    handleCommand(command);
                } catch (EndOfFileException | IOError e) {
                    // Ctrl+D, and kill signals result in exit
                    println("Exiting from the client console application.");
                    writer.flush();
                    break;
                } catch (UserInterruptException e) {
                    // Handle thread interruption for dumb terminal
                    if (!isRealTerminal(lineReader.getTerminal())) {
                        writer.flush();
                        break;
                    } else {
                        // Ctrl+C cancels the not-yet-submitted command
                        continue;
                    }
                } catch (Throwable e) {
                    e.printStackTrace(writer);
                    writer.flush();
                }
                running = running && client.getLifecycleService().isRunning();
            }
        } finally {
            IOUtil.closeResource(lineReader.getTerminal());
        }
    }

    /**
     * Handle a command.
     */
    @SuppressFBWarnings("DM_EXIT")
    @SuppressWarnings({"checkstyle:methodlength", "checkstyle:cyclomaticcomplexity", "checkstyle:npathcomplexity"})
    protected void handleCommand(String commandInputted) {
        String command = commandInputted;
        if (command == null) {
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
        command = command.trim();
        if (command.length() == 0) {
            return;
        }
        String first = command;
        int spaceIndex = command.indexOf(' ');
        String[] argsSplit = command.split(" ");
        String[] args = new String[argsSplit.length];
        for (int i = 0; i < argsSplit.length; i++) {
            args[i] = argsSplit[i].trim();
        }
        if (spaceIndex != -1) {
            first = args[0];
        }
        if (equalsIgnoreCase(first, "help")) {
            handleHelp();
        } else if (first.startsWith("#") && first.length() > 1) {
            int repeat = Integer.parseInt(first.substring(1));
            long t0 = Clock.currentTimeMillis();
            for (int i = 0; i < repeat; i++) {
                handleCommand(command.substring(first.length()).replaceAll("\\$i", "" + i));
            }
            println("ops/s = " + repeat * ONE_THOUSAND / (Clock.currentTimeMillis() - t0));
        } else if (first.startsWith("&") && first.length() > 1) {
            final int fork = Integer.parseInt(first.substring(1));
            ExecutorService pool = Executors.newFixedThreadPool(fork);
            final String threadCommand = command.substring(first.length());
            for (int i = 0; i < fork; i++) {
                final int threadID = i;
                pool.submit(new Runnable() {
                    public void run() {
                        String command = threadCommand;
                        String[] threadArgs = command.replaceAll("\\$t", "" + threadID).trim().split(" ");
                        // TODO &t #4 m.putmany x k
                        if ("m.putmany".equals(threadArgs[0]) || "m.removemany".equals(threadArgs[0])) {
                            if (threadArgs.length < LENGTH_BORDER) {
                                command += " " + Integer.parseInt(threadArgs[1]) * threadID;
                            }
                        }
                        handleCommand(command);
                    }
                });
            }
            pool.shutdown();
            try {
                // wait 1h
                pool.awaitTermination(ONE_HOUR, TimeUnit.SECONDS);
            } catch (Exception e) {
                e.printStackTrace();
            }
        } else if (first.startsWith("@")) {
            handleAt(first);
        } else if (command.indexOf(';') != -1) {
            handleColon(command);
        } else if (equalsIgnoreCase(first, "silent")) {
            silent = Boolean.parseBoolean(args[1]);
        } else if (equalsIgnoreCase(first, "echo")) {
            echo = Boolean.parseBoolean(args[1]);
            println("echo: " + echo);
        } else if (equalsIgnoreCase(first, "clear")) {
            lineReader.getTerminal().puts(InfoCmp.Capability.clear_screen);
        } else if (equalsIgnoreCase(first, "history")) {
            History hist = lineReader.getHistory();
            ListIterator<History.Entry> iterator = hist.iterator();
            while (iterator.hasNext()) {
                History.Entry entry = iterator.next();
                if (iterator.hasNext()) {
                    String entryLine = new StringBuilder()
                            .append(entry.index() + 1)
                            .append(" - ")
                            .append(entry.line())
                            .toString();
                    writer.println(entryLine);
                    writer.flush();
                } else {
                    // remove the "history" command from the history
                    iterator.remove();
                    hist.resetIndex();
                }
            }
        } else if (equalsIgnoreCase(first, "ns")) {
            handleNamespace(args);
        } else if (equalsIgnoreCase(first, "who")) {
            handleWho();
        } else if (equalsIgnoreCase(first, "jvm")) {
            handleJvm();
        } else if (lowerCaseInternal(first).contains("lock") && !first.contains(".")) {
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
            handleStats(args);
        } else if ("t.publish".equals(first)) {
            handleTopicPublish(args);
        } else if ("q.offer".equals(first)) {
            handleQOffer(args);
        } else if ("q.take".equals(first)) {
            handleQTake(args);
        } else if ("q.poll".equals(first)) {
            handleQPoll(args);
        } else if ("q.peek".equals(first)) {
            handleQPeek(args);
        } else if ("q.capacity".equals(first)) {
            handleQCapacity(args);
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
            handleAtomicNumberGet(args);
        } else if ("a.set".equals(first)) {
            handleAtomicNumberSet(args);
        } else if ("a.incrementAndGet".equals(first)) {
            handleAtomicNumberInc(args);
        } else if ("a.decrementAndGet".equals(first)) {
            handleAtomicNumberDec(args);
        } else if (first.equals("execute")) {
            execute(args);
        } else if (first.equals("partitions")) {
            handlePartitions(args);
        } else if (equalsIgnoreCase(first, "executeOnKey")) {
            executeOnKey(args);
        } else if (equalsIgnoreCase(first, "executeOnMember")) {
            executeOnMember(args);
        } else if (equalsIgnoreCase(first, "executeOnMembers")) {
            executeOnMembers(args);
        } else if (equalsIgnoreCase(first, "instances")) {
            handleInstances(args);
        } else if (equalsIgnoreCase(first, "quit") || equalsIgnoreCase(first, "exit")
                || equalsIgnoreCase(first, "shutdown")) {
            println("Exiting from the client console application.");
            writer.flush();
            System.exit(0);
        } else if (first.startsWith("e") && first.endsWith(".simulateLoad")) {
            handleExecutorSimulate(args);
        } else {
            println("type 'help' for help");
        }
    }

    private void handleExecutorSimulate(String[] args) {
        String first = args[0];
        int threadCount = Integer.parseInt(first.substring(1, first.indexOf(".")));
        if (threadCount < 1 || threadCount > MAX_THREAD_COUNT) {
            throw new RuntimeException("threadcount can't be smaller than 1 or larger than 16");
        }

        int taskCount = Integer.parseInt(args[1]);
        int durationSec = Integer.parseInt(args[2]);

        long startMs = System.currentTimeMillis();

        IExecutorService executor = client.getExecutorService(executorNamespace + ' ' + threadCount);
        List<Future> futures = new LinkedList<Future>();
        List<Member> members = new LinkedList<Member>(client.getCluster().getMembers());

        int totalThreadCount = client.getCluster().getMembers().size() * threadCount;

        int latchId = 0;
        for (int k = 0; k < taskCount; k++) {
            Member member = members.get(k % members.size());
            if (taskCount % totalThreadCount == 0) {
                latchId = taskCount / totalThreadCount;
                client.getCPSubsystem().getCountDownLatch("latch" + latchId).trySetCount(totalThreadCount);

            }
            Future f = executor.submitToMember(new SimulateLoadTask(durationSec, k + 1, "latch" + latchId), member);
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

    @SuppressFBWarnings("DM_DEFAULT_ENCODING")
    private void handleAt(String first) {
        if (first.length() == 1) {
            println("usage: @<file-name>");
            return;
        }
        File f = new File(first.substring(1));
        println("Executing script file " + f.getAbsolutePath());
        if (f.exists()) {
            BufferedReader br = null;
            try {
                br = new BufferedReader(new FileReader(f));
                String l = br.readLine();
                while (l != null) {
                    handleCommand(l);
                    l = br.readLine();
                }
            } catch (IOException e) {
                e.printStackTrace();
            } finally {
                IOUtil.closeResource(br);
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

    private void handleNamespace(String[] args) {
        if (args.length > 1) {
            namespace = args[1];
            println("namespace: " + namespace);
            //init();
        }
    }

    @SuppressFBWarnings("DM_GC")
    private void handleJvm() {
        System.gc();
        Runtime runtime = Runtime.getRuntime();
        println("Memory max: " + runtime.maxMemory() / ONE_KB / ONE_KB + 'M');
        long freeMemory = runtime.freeMemory() / ONE_KB / ONE_KB;
        int freeMemoryPercentage = (int) (runtime.freeMemory() * HUNDRED_CONSTANT / runtime.maxMemory());
        println("Memory free: " + freeMemory + "M " + freeMemoryPercentage + '%');
        long total = runtime.totalMemory();
        long free = runtime.freeMemory();
        println("Used Memory:" + ((total - free) / ONE_KB / ONE_KB) + "MB");
        println("# procs: " + RuntimeAvailableProcessors.get());
        println("OS info: " + ManagementFactory.getOperatingSystemMXBean().getArch()
                + ' ' + ManagementFactory.getOperatingSystemMXBean().getName() + ' '
                + ManagementFactory.getOperatingSystemMXBean().getVersion());
        println("JVM: " + ManagementFactory.getRuntimeMXBean().getVmVendor() + " "
                + ManagementFactory.getRuntimeMXBean().getVmName() + " "
                + ManagementFactory.getRuntimeMXBean().getVmVersion());
    }

    private void handleWho() {
        StringBuilder sb = new StringBuilder("\n\nMembers [");
        final Collection<Member> members = client.getCluster().getMembers();
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

    private void handleAtomicNumberGet(String[] args) {
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

    private void handleAtomicNumberInc(String[] args) {
        println(getAtomicNumber().incrementAndGet());
    }

    private void handleAtomicNumberDec(String[] args) {
        println(getAtomicNumber().decrementAndGet());
    }

    protected void handlePartitions(String[] args) {
        Set<Partition> partitions = client.getPartitionService().getPartitions();
        Map<Member, Integer> partitionCounts = new HashMap<Member, Integer>();
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
        Set<Entry<Member, Integer>> entries = partitionCounts.entrySet();
        for (Entry<Member, Integer> entry : entries) {
            println(entry.getKey() + ":" + entry.getValue());
        }
    }

    protected void handleInstances(String[] args) {
        Collection<DistributedObject> distributedObjects = client.getDistributedObjects();
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
            if (index >= 0) {
                println(getList().remove(index));
            } else {
                println(getList().remove(args[1]));
            }
        } catch (NumberFormatException e) {
            throw new RuntimeException(e);
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
        IList list = getList();
        int count = 1;
        if (args.length > 1) {
            count = Integer.parseInt(args[1]);
        }
        int successCount = 0;
        long t0 = Clock.currentTimeMillis();
        for (int i = 0; i < count; i++) {
            boolean success = list.add("obj" + i);
            if (success) {
                successCount++;
            }
        }
        long t1 = Clock.currentTimeMillis();
        println("Added " + successCount + " objects.");
        println("size = " + list.size() + ", " + successCount * ONE_THOUSAND / (t1 - t0)
                + " evt/s");
    }

    // ==================== map ===================================

    protected void handleMapPut(String[] args) {
        println(getMap().put(args[1], args[2]));
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
        Map<String, byte[]> theMap = new HashMap<>(count);
        for (int i = 0; i < count; i++) {
            theMap.put("key" + (start + i), value);
        }
        long t0 = Clock.currentTimeMillis();
        getMap().putAll(theMap);
        long t1 = Clock.currentTimeMillis();
        if (t1 - t0 > 1) {
            println("size = " + getMap().size() + ", " + count * ONE_THOUSAND / (t1 - t0)
                    + " evt/s, " + (count * ONE_THOUSAND / (t1 - t0)) * (b * BYTE_TO_BIT) / ONE_KB + " Kbit/s, "
                    + count * b / ONE_KB + " KB added");
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
        long t0 = Clock.currentTimeMillis();
        for (int i = 0; i < count; i++) {
            getMap().remove("key" + (start + i));
        }
        long t1 = Clock.currentTimeMillis();
        println("size = " + getMap().size() + ", " + count * ONE_THOUSAND / (t1 - t0) + " evt/s");
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

    protected void handleMapKeys() {
        Set set = getMap().keySet();
        Iterator it = set.iterator();
        int count = 0;
        while (it.hasNext()) {
            count++;
            println(it.next());
        }
        println("Total " + count);
    }

    protected void handleMapEntries() {
        Set set = getMap().entrySet();
        Iterator it = set.iterator();
        int count = 0;
        while (it.hasNext()) {
            count++;
            Entry entry = (Entry) it.next();
            println(entry.getKey() + ": " + entry.getValue());
        }
        println("Total " + count);
    }

    protected void handleMapValues() {
        Collection set = getMap().values();
        Iterator it = set.iterator();
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
        Set set = getMultiMap().keySet();
        Iterator it = set.iterator();
        int count = 0;
        while (it.hasNext()) {
            count++;
            println(it.next());
        }
        println("Total " + count);
    }

    protected void handleMultiMapEntries() {
        Set set = getMultiMap().entrySet();
        Iterator it = set.iterator();
        int count = 0;
        while (it.hasNext()) {
            count++;
            Entry entry = (Entry) it.next();
            println(entry.getKey() + ": " + entry.getValue());
        }
        println("Total " + count);
    }

    protected void handleMultiMapValues() {
        Collection set = getMultiMap().values();
        Iterator it = set.iterator();
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

    private void handleStats(String[] args) {
        String iteratorStr = args[0];
        if (iteratorStr.startsWith("m.")) {
            println(getMap().getLocalMapStats());
        }
    }

    // squid:S2222 suppression avoids sonar analysis bug regarding already known lock release issue
    @SuppressWarnings({"LockAcquiredButNotSafelyReleased", "squid:S2222"})
    protected void handleLock(String[] args) {
        String lockStr = args[0];
        String name = args[1];
        Lock lock = client.getCPSubsystem().getLock(name);
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
                    e.printStackTrace();
                    currentThread().interrupt();
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
        long t0 = Clock.currentTimeMillis();
        for (int i = 0; i < count; i++) {
            boolean success = getSet().add("obj" + i);
            if (success) {
                successCount++;
            }
        }
        long t1 = Clock.currentTimeMillis();
        println("Added " + successCount + " objects.");
        println("size = " + getSet().size() + ", " + successCount * ONE_THOUSAND / (t1 - t0)
                + " evt/s");
    }

    protected void handleSetRemoveMany(String[] args) {
        int count = 1;
        if (args.length > 1) {
            count = Integer.parseInt(args[1]);
        }
        int successCount = 0;
        long t0 = Clock.currentTimeMillis();
        for (int i = 0; i < count; i++) {
            boolean success = getSet().remove("obj" + i);
            if (success) {
                successCount++;
            }
        }
        long t1 = Clock.currentTimeMillis();
        println("Removed " + successCount + " objects.");
        println("size = " + getSet().size() + ", " + successCount * ONE_THOUSAND / (t1 - t0)
                + " evt/s");
    }


    protected void handleIterator(String[] args) {
        Iterator it = null;
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
        println("Size = " + size);
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

    protected void handleQTake(String[] args) {
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
        long t0 = Clock.currentTimeMillis();
        for (int i = 0; i < count; i++) {
            if (value == null) {
                getQueue().offer("obj");
            } else {
                getQueue().offer(value);
            }
        }
        long t1 = Clock.currentTimeMillis();
        print("size = " + getQueue().size() + ", " + count * ONE_THOUSAND / (t1 - t0) + " evt/s");
        if (value == null) {
            println("");
        } else {
            int b = Integer.parseInt(args[2]);
            println(", " + (count * ONE_THOUSAND / (t1 - t0)) * (b * BYTE_TO_BIT) / ONE_KB + " Kbit/s, "
                    + count * b / ONE_KB + " KB added");
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
            if (obj instanceof byte[]) {
                println(c++ + " " + ((byte[]) obj).length);
            } else {
                println(c++ + " " + obj);
            }
        }
    }

    protected void handleQPeek(String[] args) {
        println(getQueue().peek());
    }

    protected void handleQCapacity(String[] args) {
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
            IExecutorService executorService = client.getExecutorService("default");
            Echo callable = new Echo(args[1]);
            Future<String> future;
            if (onKey) {
                String key = args[2];
                future = executorService.submitToKeyOwner(callable, key);
            } else if (onMember) {
                int memberIndex = Integer.parseInt(args[2]);
                List<Member> members = new LinkedList<>(client.getCluster().getMembers());
                if (memberIndex >= members.size()) {
                    throw new IndexOutOfBoundsException("Member index: " + memberIndex + " must be smaller than " + members
                            .size());
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
            IExecutorService executorService = client.getExecutorService("default");
            Echo task = new Echo(args[1]);
            Map<Member, Future<String>> results = executorService.submitToAllMembers(task);

            for (Future f : results.values()) {
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
    public void entryAdded(EntryEvent event) {
        println(event);
    }

    @Override
    public void entryRemoved(EntryEvent event) {
        println(event);
    }

    @Override
    public void entryUpdated(EntryEvent event) {
        println(event);
    }

    @Override
    public void entryEvicted(EntryEvent event) {
        println(event);
    }

    @Override
    public void entryExpired(EntryEvent event) {
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
     * Handles the help command.
     */
    protected void handleHelp() {
        boolean silentBefore = silent;
        silent = false;
        printlnBold("Commands:");

        printGeneralCommands();
        printMapCommands();
        printQueueCommands();
        printSetCommands();
        printLockCommands();
        printMulitiMapCommands();
        printListCommands();
        printAtomicLongCommands();
        printExecutorServiceCommands();

        silent = silentBefore;
    }

    private void printGeneralCommands() {
        printlnBold("General commands:");
        println("  echo true|false                         turns on/off echo of commands (default false)");
        println("  clear                                   clears the terminal screen");
        println("  exit                                    exits from the client console app.");
        println("  silent true|false                       turns on/off silent of command output (default false)");
        println("  #<number> <command>                     repeats <number> time <command>, replace $i in");
        println("                                            <command> with current iteration (0..<number-1>)");
        println("  &<number> <command>                     forks <number> threads to execute <command>, replace");
        println("                                            $t in <command> with current thread number (0..<number-1>).");
        println("                                            When using #x or &x, it is advised to use silent true");
        println("                                            as well. When using &x with m.putmany and m.removemany,");
        println("                                            each thread will get a different share of keys unless");
        println("                                            a start key index is specified.");
        println("  history                                 shows the command history of the current session");
        println("  jvm                                     displays info about the runtime");
        println("  who                                     displays info about the cluster");
        println("  ns <string>                             switch the namespace for using the distributed");
        println("                                            queue/map/set/list <string> (defaults to \"default\")");
        println("  @<file>                                 executes the given <file> script. Use '//' for");
        println("                                            comments in the script");
        println("");
    }

    private void printQueueCommands() {
        printlnBold("Queue commands:");
        println("  q.offer <string>                        adds a string object to the queue");
        println("  q.poll                                  takes an object from the queue");
        println("  q.offermany <number> [<size>]           adds indicated number of string objects to the");
        println("                                            queue ('obj<i>' or  byte[<size>]) ");
        println("  q.pollmany <number>                     takes indicated number of objects from the queue");
        println("  q.iterator [remove]                     iterates the queue, remove if specified");
        println("  q.size                                  size of the queue");
        println("  q.clear                                 clears the queue");
        println("");
    }

    private void printSetCommands() {
        printlnBold("Set commands:");
        println("  s.add <string>                          adds a string object to the set");
        println("  s.remove <string>                       removes the string object from the set");
        println("  s.addmany <number>                      adds indicated number of string objects");
        println("                                            to the set ('obj<i>')");
        println("  s.removemany <number>                   takes indicated number of objects from the set");
        println("  s.iterator [remove]                     iterates the set, removes if specified");
        println("  s.size                                  size of the set");
        println("  s.clear                                 clears the set");
        println("");
    }

    private void printLockCommands() {
        printlnBold("Lock commands:");
        println("  These lock commands demonstrate the usage of Hazelcast's linearizable, distributed, and reentrant");
        println("  implementation of Lock. For more information, see `com.hazelcast.cp.lock.FencedLock`.");
        println("  lock <name>                             acquires the lock with the given name");
        println("  tryLock <name>                          acquires the lock only if it is free at the time");
        println("                                            of invocation");
        println("  tryLock <name> <time>                   acquires the lock if it is free within the given");
        println("                                            waiting time");
        println("  unlock <name>                           releases the lock if the lock is currently held");
        println("                                            by the current thread");
        println("");
    }

    private void printMapCommands() {
        printlnBold("Map commands:");
        println("  m.put <key> <value>                     puts an entry to the map");
        println("  m.remove <key>                          removes the entry of given key from the map");
        println("  m.get <key>                             returns the value of given key from the map");
        println("  m.putmany <number> [<size>] [<index>]   puts indicated number of entries to the map:");
        println("                                            ('key<i>':byte[<size>], <index>+(0..<number>)");
        println("  m.removemany <number> [<index>]         removes indicated number of entries from the map");
        println("                                            ('key<i>', <index>+(0..<number>)");
        println("                                            When using &x with m.putmany and m.removemany, each");
        println("                                            thread will get a different share of keys unless a");
        println("                                            start key <index> is specified");
        println("  m.keys                                  iterates the keys of the map");
        println("  m.values                                iterates the values of the map");
        println("  m.entries                               iterates the entries of the map");
        println("  m.iterator [remove]                     iterates the keys of the map, remove if specified");
        println("  m.size                                  size of the map");
        println("  m.clear                                 clears the map");
        println("  m.destroy                               destroys the map");
        println("  m.lock <key>                            locks the key");
        println("  m.tryLock <key>                         tries to lock the key and returns immediately");
        println("  m.tryLock <key> <time>                  tries to lock the key within given seconds");
        println("  m.unlock <key>                          unlocks the key");
        println("  m.stats                                 shows the local stats of the map");
        println("");
    }

    private void printMulitiMapCommands() {
        printlnBold("MultiMap commands:");
        println("  mm.put <key> <value>                    puts an entry to the multimap");
        println("  mm.get <key>                            returns the value of given key from the multimap");
        println("  mm.remove <key>                         removes the entry of given key from the multimap");
        println("  mm.size                                 size of the multimap");
        println("  mm.clear                                clears the multimap");
        println("  mm.destroy                              destroys the multimap");
        println("  mm.iterator [remove]                    iterates the keys of the multimap, remove if specified");
        println("  mm.keys                                 iterates the keys of the multimap");
        println("  mm.values                               iterates the values of the multimap");
        println("  mm.entries                              iterates the entries of the multimap");
        println("  mm.lock <key>                           locks the key");
        println("  mm.tryLock <key>                        tries to lock the key and returns immediately");
        println("  mm.tryLock <key> <time>                 tries to lock the key within given seconds");
        println("  mm.unlock <key>                         unlocks the key");
        println("");
    }

    private void printExecutorServiceCommands() {
        printlnBold("Executor Service commands:");
        println("  execute <echo-input>                    executes an echo task on random member");
        println("  executeOnKey <echo-input> <key>         executes an echo task on the member that");
        println("                                            owns the given key");
        println("  executeOnMembers <echo-input>           executes an echo task on all of the members");
        println("  executeOnMembers <echo-input>           executes an echo task on the member with");
        println("                     <memberIndex>          given index");
        println("  e<threadcount>.simulateLoad             simulates load on executor with given number");
        println("          <task-count> <delaySeconds>       of thread (e1..e16)");
        println("");
    }

    private void printAtomicLongCommands() {
        printlnBold("IAtomicLong commands:");
        println("  a.get                                   gets the current value of atomic long");
        println("  a.set <long>                            atomically sets the given value");
        println("  a.incrementAndGet                       atomically increment the current value by");
        println("                                            one and then gets the resulting value");
        println("  a.decrementAndGet                       atomically decrement the current value by");
        println("                                            one and then gets the resulting value");
        println("");
    }

    private void printListCommands() {
        printlnBold("List commands:");
        println("  l.add <string>                          adds the given string object to the end of the");
        println("                                            list");
        println("  l.add <index> <string>                  adds the given string object to the specified");
        println("                                             position in the list");
        println("  l.contains <string>                     checks whether if the given string presents");
        println("                                            in the list");
        println("  l.remove <string>                       removes the first occurrence of the given string");
        println("  l.remove <index>                        removes the string element from the specified");
        println("                                             position of the list");
        println("  l.set <index> <string>                  replaces the element at the specified position");
        println("                                            with given string");
        println("  l.iterator [remove]                     iterates over the items of the list, remove");
        println("                                            if specified");
        println("  l.size                                  returns the number of element in the list");
        println("  l.clear                                 removes all items from the list");
        println("");
    }

    public void println(Object obj) {
        if (!silent) {
            writer.println(obj);
        }
    }

    public void print(Object obj) {
        if (!silent) {
            writer.print(obj);
        }
    }

    public void printlnBold(Object obj) {
        if (!silent) {
            writer.println(new AttributedStringBuilder()
                    .style(AttributedStyle.BOLD)
                    .append(String.valueOf(obj))
                    .toAnsi());
        }
    }

    private static String startPrompt(HazelcastInstance hz) {
        HazelcastClientInstanceImpl hazelcastClientImpl = getHazelcastClientInstanceImpl(hz);
        ClientClusterService clientClusterService = hazelcastClientImpl.getClientClusterService();
        MCClusterMetadata clusterMetadata =
                FutureUtil.getValue(getClusterMetadata(hazelcastClientImpl, clientClusterService.getMasterMember()));
        Cluster cluster = hazelcastClientImpl.getCluster();
        Set<Member> members = cluster.getMembers();
        String versionString = "Hazelcast " + clusterMetadata.getMemberVersion();
        return new StringBuilder()
                .append("Hazelcast Console Application has started.\n")
                .append("Connected to ")
                .append(versionString)
                .append(" at ")
                .append(members.iterator().next().getAddress().toString())
                .append(" (+")
                .append(members.size() - 1)
                .append(" more)\n")
                .append("Type 'help' for instructions").toString();
    }

    /**
     * Starts the hazelcast console application.
     */
    public static void run(HazelcastInstance client) {
        ClientConsoleApp clientConsoleApp = new ClientConsoleApp(client);
        clientConsoleApp.start();
    }

    /**
     * This main function should only be used for test runs
     */
    public static void main(String[] args) {
        run(HazelcastClient.newHazelcastClient());
    }
}
