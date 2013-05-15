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

package com.hazelcast.examples;

import com.hazelcast.core.*;
import com.hazelcast.core.Partition;
import com.hazelcast.util.Clock;

import java.io.*;
import java.lang.management.ManagementFactory;
import java.util.*;
import java.util.Map.Entry;
import java.util.concurrent.*;
import java.util.concurrent.locks.Lock;

/**
 * Special thanks to Alexandre Vasseur for providing this very nice test
 * application.
 *
 * @author alex, talip
 */
public class TestApp implements EntryListener, ItemListener, MessageListener {

    private IQueue<Object> queue = null;

    private ITopic<Object> topic = null;

    private IMap<Object, Object> map = null;

    private ISet<Object> set = null;

    private IList<Object> list = null;

    private IAtomicLong atomicNumber;

    private String namespace = "default";

    private boolean silent = false;

    private boolean echo = false;

    private volatile HazelcastInstance hazelcast;

    private volatile LineReader lineReader;

    private volatile boolean running = false;

    public TestApp(HazelcastInstance hazelcast) {
        this.hazelcast = hazelcast;
    }

    public IQueue<Object> getQueue() {
//        if (queue == null) {
        queue = hazelcast.getQueue(namespace);
//        }
        return queue;
    }

    public ITopic<Object> getTopic() {
//        if (topic == null) {
        topic = hazelcast.getTopic(namespace);
//        }
        return topic;
    }

    public IMap<Object, Object> getMap() {
//        if (map == null) {
        map = hazelcast.getMap(namespace);
//        }
        return map;
    }

    public IAtomicLong getAtomicNumber() {
        atomicNumber = hazelcast.getAtomicLong(namespace);
        return atomicNumber;
    }

    public ISet<Object> getSet() {
//        if (set == null) {
        set = hazelcast.getSet(namespace);
//        }
        return set;
    }

    public IList<Object> getList() {
//        if (list == null) {
        list = hazelcast.getList(namespace);
//        }
        return list;
    }

    public void setHazelcast(HazelcastInstance hazelcast) {
        this.hazelcast = hazelcast;
        map = null;
        list = null;
        set = null;
        queue = null;
        topic = null;
    }

    public static void main(String[] args) throws Exception {
        TestApp testApp = new TestApp(Hazelcast.newHazelcastInstance(null));
        testApp.start(args);
    }

    public void stop() {
        running = false;
    }

    public void start(String[] args) throws Exception {
        if (lineReader == null) {
            lineReader = new DefaultLineReader();
        }
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

    public void setLineReader(LineReader lineReader) {
        this.lineReader = lineReader;
    }

    class DefaultLineReader implements LineReader {

        BufferedReader in = new BufferedReader(new InputStreamReader(System.in));

        public String readLine() throws Exception {
            return in.readLine();
        }
    }

    class HistoricLineReader implements LineReader {
        InputStream in = System.in;

        public String readLine() throws Exception {
            while (true) {
                System.in.read();
                println("char " + System.in.read());
            }
        }

        int readCharacter() throws Exception {
            return in.read();
        }
    }



    protected void handleCommand(String command) {
        if(command.contains("__")) {
        namespace = command.split("__")[0];
        command = command.substring(command.indexOf("__")+2);
        }

        if (echo) {
            if (Thread.currentThread().getName().toLowerCase().indexOf("main") < 0)
                println(" [" + Thread.currentThread().getName() + "] " + command);
            else
                println(command);
        }
        if (command == null || command.startsWith("//"))
            return;
        command = command.trim();
        if (command == null || command.length() == 0) {
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
        if (command.startsWith("help")) {
            handleHelp(command);
        } else if (first.startsWith("#") && first.length() > 1) {
            int repeat = Integer.parseInt(first.substring(1));
            long t0 = Clock.currentTimeMillis();
            for (int i = 0; i < repeat; i++) {
                handleCommand(command.substring(first.length()).replaceAll("\\$i", "" + i));
            }
            println("ops/s = " + repeat * 1000 / (Clock.currentTimeMillis() - t0));
            return;
        } else if (first.startsWith("&") && first.length() > 1) {
            final int fork = Integer.parseInt(first.substring(1));
            ExecutorService pool = Executors.newFixedThreadPool(fork);
            final String threadCommand = command.substring(first.length());
            for (int i = 0; i < fork; i++) {
                final int threadID = i;
                pool.submit(new Runnable() {
                    public void run() {
                        String command = threadCommand;
                        String[] threadArgs = command.replaceAll("\\$t", "" + threadID).trim()
                                .split(" ");
                        // TODO &t #4 m.putmany x k
                        if ("m.putmany".equals(threadArgs[0])
                                || "m.removemany".equals(threadArgs[0])) {
                            if (threadArgs.length < 4) {
                                command += " " + Integer.parseInt(threadArgs[1]) * threadID;
                            }
                        }
                        handleCommand(command);
                    }
                });
            }
            pool.shutdown();
            try {
                pool.awaitTermination(60 * 60, TimeUnit.SECONDS);// wait 1h
            } catch (Exception e) {
                e.printStackTrace();
            }
        } else if (first.startsWith("@")) {
            if (first.length() == 1) {
                println("usage: @<file-name>");
                return;
            }
            File f = new File(first.substring(1));
            println("Executing script file " + f.getAbsolutePath());
            if (f.exists()) {
                try {
                    BufferedReader br = new BufferedReader(new FileReader(f));
                    String l = br.readLine();
                    while (l != null) {
                        handleCommand(l);
                        l = br.readLine();
                    }
                    br.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            } else {
                println("File not found! " + f.getAbsolutePath());
            }
        } else if (command.indexOf(';') != -1) {
            StringTokenizer st = new StringTokenizer(command, ";");
            while (st.hasMoreTokens()) {
                handleCommand(st.nextToken());
            }
            return;
        } else if ("silent".equals(first)) {
            silent = Boolean.parseBoolean(args[1]);
        } else if ("shutdown".equals(first)) {
            hazelcast.getLifecycleService().shutdown();
        } else if ("echo".equals(first)) {
            echo = Boolean.parseBoolean(args[1]);
            println("echo: " + echo);
        } else if ("ns".equals(first)) {
            if (args.length > 1) {
                namespace = args[1];
                println("namespace: " + namespace);
//                init();
            }
        } else if ("whoami".equals(first)) {
            println(hazelcast.getCluster().getLocalMember());
        } else if ("who".equals(first)) {
            println(hazelcast.getCluster());
        } else if ("jvm".equals(first)) {
            System.gc();
            println("Memory max: " + Runtime.getRuntime().maxMemory() / 1024 / 1024
                    + "M");
            println("Memory free: "
                    + Runtime.getRuntime().freeMemory()
                    / 1024
                    / 1024
                    + "M "
                    + (int) (Runtime.getRuntime().freeMemory() * 100 / Runtime.getRuntime()
                    .maxMemory()) + "%");
            long total = Runtime.getRuntime().totalMemory();
            long free = Runtime.getRuntime().freeMemory();
            println("Used Memory:" + ((total - free) / 1024 / 1024) + "MB");
            println("# procs: " + Runtime.getRuntime().availableProcessors());
            println("OS info: " + ManagementFactory.getOperatingSystemMXBean().getArch()
                    + " " + ManagementFactory.getOperatingSystemMXBean().getName() + " "
                    + ManagementFactory.getOperatingSystemMXBean().getVersion());
            println("JVM: " + ManagementFactory.getRuntimeMXBean().getVmVendor() + " "
                    + ManagementFactory.getRuntimeMXBean().getVmName() + " "
                    + ManagementFactory.getRuntimeMXBean().getVmVersion());
        } else if (first.indexOf("ock") != -1 && first.indexOf(".") == -1) {
            handleLock(args);
        } else if (first.indexOf(".size") != -1) {
            handleSize(args);
        } else if (first.indexOf(".clear") != -1) {
            handleClear(args);
        } else if (first.indexOf(".destroy") != -1) {
            handleDestroy(args);
        } else if (first.indexOf(".iterator") != -1) {
            handleIterator(args);
        } else if (first.indexOf(".contains") != -1) {
            handleContains(args);
        } else if (first.indexOf(".stats") != -1) {
            handStats(args);
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
        } else if (first.equalsIgnoreCase("m.putIfAbsent")) {
            handleMapPutIfAbsent(args);
        } else if (first.equals("m.putAsync")) {
            handleMapPutAsync(args);
        } else if (first.equals("m.getAsync")) {
            handleMapGetAsync(args);
        } else if (first.equals("m.put")) {
            handleMapPut(args);
        } else if (first.equals("m.get")) {
            handleMapGet(args);
        } else if (first.equalsIgnoreCase("m.getMapEntry")) {
            handleMapGetMapEntry(args);
        } else if (first.equals("m.remove")) {
            handleMapRemove(args);
        } else if (first.equals("m.evict")) {
            handleMapEvict(args);
        } else if (first.equals("m.putmany") || first.equalsIgnoreCase("m.putAll")) {
            handleMapPutMany(args);
        } else if (first.equals("m.getmany")) {
            handleMapGetMany(args);
        } else if (first.equals("m.removemany")) {
            handleMapRemoveMany(args);
        } else if (command.equalsIgnoreCase("m.localKeys")) {
            handleMapLocalKeys();
        } else if (command.equals("m.keys")) {
            handleMapKeys();
        } else if (command.equals("m.values")) {
            handleMapValues();
        } else if (command.equals("m.entries")) {
            handleMapEntries();
        } else if (first.equals("m.lock")) {
            handleMapLock(args);
        } else if (first.equalsIgnoreCase("m.tryLock")) {
            handleMapTryLock(args);
        } else if (first.equals("m.unlock")) {
            handleMapUnlock(args);
        } else if (first.indexOf(".addListener") != -1) {
            handleAddListener(args);
        } else if (first.equals("m.removeMapListener")) {
            handleRemoveListener(args);
        } else if (first.equals("m.unlock")) {
            handleMapUnlock(args);
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
        } else if ("a.inc".equals(first)) {
            handleAtomicNumberInc(args);
        } else if ("a.dec".equals(first)) {
            handleAtomicNumberDec(args);
//        } else if (first.equals("execute")) {
//            execute(args);
        } else if (first.equals("partitions")) {
            handlePartitions(args);
//        } else if (first.equals("txn")) {
//            hazelcast.getTransaction().begin();
//        } else if (first.equals("commit")) {
//            hazelcast.getTransaction().commit();
//        } else if (first.equals("rollback")) {
//            hazelcast.getTransaction().rollback();
//        } else if (first.equalsIgnoreCase("executeOnKey")) {
//            executeOnKey(args);
//        } else if (first.equalsIgnoreCase("executeOnMember")) {
//            executeOnMember(args);
//        } else if (first.equalsIgnoreCase("executeOnMembers")) {
//            executeOnMembers(args);
//        } else if (first.equalsIgnoreCase("longOther") || first.equalsIgnoreCase("executeLongOther")) {
//            executeLongTaskOnOtherMember(args);
//        } else if (first.equalsIgnoreCase("long") || first.equalsIgnoreCase("executeLong")) {
//            executeLong(args);
        } else if (first.equalsIgnoreCase("instances")) {
            handleInstances(args);
        } else if (first.equalsIgnoreCase("quit") || first.equalsIgnoreCase("exit")) {
            System.exit(0);
        } else {
            println("type 'help' for help");
        }
    }

    private void handleAtomicNumberGet(String[] args) {
        println(getAtomicNumber().get());
    }

    private void handleAtomicNumberSet(String[] args) {
        long v = 0;
        if (args.length > 1)
            v = Long.valueOf(args[1]);
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
        Set<Partition> partitions = hazelcast.getPartitionService().getPartitions();
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
        Set<Map.Entry<Member, Integer>> entries = partitionCounts.entrySet();
        for (Map.Entry<Member, Integer> entry : entries) {
            println(entry.getKey() + ":" + entry.getValue());
        }
    }

    protected void handleInstances(String[] args) {
        Collection<DistributedObject> distributedObjects = hazelcast.getDistributedObjects();
        for (DistributedObject distributedObject : distributedObjects) {
            println(distributedObject);
        }
    }

    protected void handleListContains(String[] args) {
        println(getList().contains(args[1]));
    }

    protected void handleListRemove(String[] args) {
        int index = -1;
        try {
            index = Integer.parseInt(args[1]);
        } catch (NumberFormatException e) {
            // NOP
        }
        if (index >= 0) {
            println(getList().remove(index));
        } else {
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

    protected void handleMapPut(String[] args) {
        println(getMap().put(args[1], args[2]));
    }

    protected void handleMapPutAsync(String[] args) {
        try {
            println(getMap().putAsync(args[1], args[2]).get());
        } catch (InterruptedException e) {
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
            println(getMap().getAsync(args[1]).get());
        } catch (InterruptedException e) {
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

    protected void handleMapEvict(String[] args) {
        println(getMap().evict(args[1]));
    }

    protected void handleMapPutMany(String[] args) {
        int count = 1;
        if (args.length > 1)
            count = Integer.parseInt(args[1]);
        int b = 100;
        byte[] value = new byte[b];
        if (args.length > 2) {
            b = Integer.parseInt(args[2]);
            value = new byte[b];
        }
        int start = getMap().size();
        if (args.length > 3) {
            start = Integer.parseInt(args[3]);
        }
        Map theMap = new HashMap(count);
        for (int i = 0; i < count; i++) {
            theMap.put("key" + (start + i), value);
        }
        long t0 = Clock.currentTimeMillis();
        getMap().putAll(theMap);
        long t1 = Clock.currentTimeMillis();
        if (t1 - t0 > 1) {
            println("size = " + getMap().size() + ", " + count * 1000 / (t1 - t0)
                    + " evt/s, " + (count * 1000 / (t1 - t0)) * (b * 8) / 1024 + " Kbit/s, "
                    + count * b / 1024 + " KB added");
        }
    }

    private void handStats(String[] args) {
        String iteratorStr = args[0];
        if (iteratorStr.startsWith("s.")) {
        } else if (iteratorStr.startsWith("m.")) {
            println(getMap().getLocalMapStats());
        } else if (iteratorStr.startsWith("q.")) {
            println(getQueue().getLocalQueueStats());
        } else if (iteratorStr.startsWith("l.")) {
        }
    }

    protected void handleMapGetMany(String[] args) {
        int count = 1;
        if (args.length > 1)
            count = Integer.parseInt(args[1]);
        for (int i = 0; i < count; i++) {
            println(getMap().get("key" + i));
        }
    }

    protected void handleMapRemoveMany(String[] args) {
        int count = 1;
        if (args.length > 1)
            count = Integer.parseInt(args[1]);
        int start = 0;
        if (args.length > 2)
            start = Integer.parseInt(args[2]);
        long t0 = Clock.currentTimeMillis();
        for (int i = 0; i < count; i++) {
            getMap().remove("key" + (start + i));
        }
        long t1 = Clock.currentTimeMillis();
        println("size = " + getMap().size() + ", " + count * 1000 / (t1 - t0) + " evt/s");
    }

    protected void handleMapLock(String[] args) {
        getMap().lock(args[1]);
        println("true");
    }

    @SuppressWarnings("LockAcquiredButNotSafelyReleased")
    protected void handleLock(String[] args) {
        String lockStr = args[0];
        String key = args[1];
        Lock lock = hazelcast.getLock(key);
        if (lockStr.equalsIgnoreCase("lock")) {
            lock.lock();
            println("true");
        } else if (lockStr.equalsIgnoreCase("unlock")) {
            lock.unlock();
            println("true");
        } else if (lockStr.equalsIgnoreCase("trylock")) {
            String timeout = args.length > 2 ? args[2] : null;
            if (timeout == null) {
                println(lock.tryLock());
            } else {
                long time = Long.valueOf(timeout);
                try {
                    println(lock.tryLock(time, TimeUnit.SECONDS));
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        }
    }

    protected void handleMapTryLock(String[] args) {
        String key = args[1];
        long time = (args.length > 2) ? Long.valueOf(args[2]) : 0;
        boolean locked = false;
        if (time == 0)
            locked = getMap().tryLock(key);
        else
            try {
                locked = getMap().tryLock(key, time, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                locked = false;
            }
        println(locked);
    }

    protected void handleMapUnlock(String[] args) {
        getMap().unlock(args[1]);
        println("true");
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

    protected void handleMapLocalKeys() {
        Set set = getMap().localKeySet();
        Iterator it = set.iterator();
        int count = 0;
        while (it.hasNext()) {
            count++;
            println(it.next());
        }
        println("Total " + count);
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
        long time = Clock.currentTimeMillis();
        while (it.hasNext()) {
            count++;
            Map.Entry entry = (Entry) it.next();
            println(entry.getKey() + " : " + entry.getValue());
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

    protected void handleSetAdd(String[] args) {
        println(getSet().add(args[1]));
    }

    protected void handleSetRemove(String[] args) {
        println(getSet().remove(args[1]));
    }

    protected void handleSetAddMany(String[] args) {
        int count = 1;
        if (args.length > 1)
            count = Integer.parseInt(args[1]);
        int successCount = 0;
        long t0 = Clock.currentTimeMillis();
        for (int i = 0; i < count; i++) {
            boolean success = getSet().add("obj" + i);
            if (success)
                successCount++;
        }
        long t1 = Clock.currentTimeMillis();
        println("Added " + successCount + " objects.");
        println("size = " + getSet().size() + ", " + successCount * 1000 / (t1 - t0)
                + " evt/s");
    }

    protected void handleListAddMany(String[] args) {
        int count = 1;
        if (args.length > 1)
            count = Integer.parseInt(args[1]);
        int successCount = 0;
        long t0 = Clock.currentTimeMillis();
        for (int i = 0; i < count; i++) {
            boolean success = getList().add("obj" + i);
            if (success)
                successCount++;
        }
        long t1 = Clock.currentTimeMillis();
        println("Added " + successCount + " objects.");
        println("size = " + list.size() + ", " + successCount * 1000 / (t1 - t0)
                + " evt/s");
    }

    protected void handleSetRemoveMany(String[] args) {
        int count = 1;
        if (args.length > 1)
            count = Integer.parseInt(args[1]);
        int successCount = 0;
        long t0 = Clock.currentTimeMillis();
        for (int i = 0; i < count; i++) {
            boolean success = getSet().remove("obj" + i);
            if (success)
                successCount++;
        }
        long t1 = Clock.currentTimeMillis();
        println("Removed " + successCount + " objects.");
        println("size = " + getSet().size() + ", " + successCount * 1000 / (t1 - t0)
                + " evt/s");
    }

    protected void handleIterator(String[] args) {
        Iterator it = null;
        String iteratorStr = args[0];
        if (iteratorStr.startsWith("s.")) {
            it = getSet().iterator();
        } else if (iteratorStr.startsWith("m.")) {
            it = getMap().keySet().iterator();
        } else if (iteratorStr.startsWith("q.")) {
            it = getQueue().iterator();
        } else if (iteratorStr.startsWith("l.")) {
            it = getList().iterator();
        }
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

    protected void handleContains(String[] args) {
        String iteratorStr = args[0];
        boolean key = false;
        boolean value = false;
        if (iteratorStr.toLowerCase().endsWith("key")) {
            key = true;
        } else if (iteratorStr.toLowerCase().endsWith("value")) {
            value = true;
        }
        String data = args[1];
        boolean result = false;
        if (iteratorStr.startsWith("s.")) {
            result = getSet().contains(data);
        } else if (iteratorStr.startsWith("m.")) {
            result = (key) ? getMap().containsKey(data) : getMap().containsValue(data);
        } else if (iteratorStr.startsWith("q.")) {
            result = getQueue().contains(data);
        } else if (iteratorStr.startsWith("l.")) {
            result = getList().contains(data);
        }
        println("Contains : " + result);
    }

    protected void handleSize(String[] args) {
        int size = 0;
        String iteratorStr = args[0];
        if (iteratorStr.startsWith("s.")) {
            size = getSet().size();
        } else if (iteratorStr.startsWith("m.")) {
            size = getMap().size();
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
            timeout = Long.valueOf(args[2]);
        }
        try {
            boolean offered = getQueue().offer(args[1], timeout, TimeUnit.SECONDS);
            println(offered);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    protected void handleQTake(String[] args) {
        try {
            println(getQueue().take());
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    protected void handleQPoll(String[] args) {
        long timeout = 0;
        if (args.length > 1) {
            timeout = Long.valueOf(args[1]);
        }
        try {
            println(getQueue().poll(timeout, TimeUnit.SECONDS));
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    protected void handleTopicPublish(String[] args) {
        getTopic().publish(args[1]);
    }

    protected void handleQOfferMany(String[] args) {
        int count = 1;
        if (args.length > 1)
            count = Integer.parseInt(args[1]);
        Object value = null;
        if (args.length > 2)
            value = new byte[Integer.parseInt(args[2])];
        long t0 = Clock.currentTimeMillis();
        for (int i = 0; i < count; i++) {
            if (value == null)
                getQueue().offer("obj");
            else
                getQueue().offer(value);
        }
        long t1 = Clock.currentTimeMillis();
        print("size = " + getQueue().size() + ", " + count * 1000 / (t1 - t0) + " evt/s");
        if (value == null) {
            println("");
        } else {
            int b = Integer.parseInt(args[2]);
            println(", " + (count * 1000 / (t1 - t0)) * (b * 8) / 1024 + " Kbit/s, "
                    + count * b / 1024 + " KB added");
        }
    }

    protected void handleQPollMany(String[] args) {
        int count = 1;
        if (args.length > 1)
            count = Integer.parseInt(args[1]);
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

//    private void execute(String[] args) {
//        // execute <echo-string>
//        doExecute(false, false, args);
//    }
//
//    private void executeOnKey(String[] args) {
//        // executeOnKey <echo-string> <key>
//        doExecute(true, false, args);
//    }
//
//    private void executeOnMember(String[] args) {
//        // executeOnMember <echo-string> <memberIndex>
//        doExecute(false, true, args);
//    }
//
//    private void ex(String input) throws Exception {
//        FutureTask<String> task = new DistributedTask<String>(new Echo(input));
//        ExecutorService executorService = hazelcast.getExecutorService("default");
//        executorService.execute(task);
//        String echoResult = task.get();
//    }
//
//    private void doExecute(boolean onKey, boolean onMember, String[] args) {
//        // executeOnKey <echo-string> <key>
//        try {
//            ExecutorService executorService = hazelcast.getExecutorService("default");
//            Echo callable = new Echo(args[1]);
//            FutureTask<String> task = null;
//            if (onKey) {
//                String key = args[2];
//                task = new DistributedTask<String>(callable, key);
//            } else if (onMember) {
//                int memberIndex = Integer.parseInt(args[2]);
//                Member member = (Member) hazelcast.getCluster().getMembers().toArray()[memberIndex];
//                task = new DistributedTask<String>(callable, member);
//            } else {
//                task = new DistributedTask<String>(callable);
//            }
//            executorService.execute(task);
//            println("Result: " + task.get());
//        } catch (InterruptedException e) {
//            e.printStackTrace();
//        } catch (ExecutionException e) {
//            e.printStackTrace();
//        }
//    }
//
//    private void executeOnMembers(String[] args) {
//        // executeOnMembers <echo-string>
//        try {
//            ExecutorService executorService = hazelcast.getExecutorService("default");
//            MultiTask<String> echoTask = new MultiTask(new Echo(args[1]), hazelcast.getCluster()
//                    .getMembers());
//            executorService.execute(echoTask);
//            Collection<String> results = echoTask.get();
//            for (String result : results) {
//                println(result);
//            }
//        } catch (InterruptedException e) {
//            e.printStackTrace();
//        } catch (ExecutionException e) {
//            e.printStackTrace();
//        }
//    }
//
//    private void executeLong(String[] args) {
//        // executeOnMembers <echo-string>
//        try {
//            ExecutorService executorService = hazelcast.getExecutorService("default");
//            MultiTask<String> echoTask = new MultiTask(new LongTask(args[1]), hazelcast.getCluster()
//                    .getMembers()) {
//                @Override
//                public void setMemberLeft(Member member) {
//                    println("Member Left " + member);
//                }
//
//                @Override
//                public void done() {
//                    println("Done!");
//                }
//            };
//            executorService.execute(echoTask);
//            Collection<String> results = echoTask.get();
//            for (String result : results) {
//                println(result);
//            }
//        } catch (Exception e) {
//            e.printStackTrace();
//        }
//    }
//
//    private void executeLongTaskOnOtherMember(String[] args) {
//        // executeOnMembers <echo-string>
//        try {
//            ExecutorService executorService = hazelcast.getExecutorService("default");
//            Member otherMember = null;
//            Set<Member> members = hazelcast.getCluster().getMembers();
//            for (Member member : members) {
//                if (!member.localMember()) {
//                    otherMember = member;
//                }
//            }
//            if (otherMember == null) {
//                otherMember = hazelcast.getCluster().getLocalMember();
//            }
//            DistributedTask<String> echoTask = new DistributedTask(new LongTask(args[1]), otherMember) {
//                @Override
//                public void setMemberLeft(Member member) {
//                    println("Member Left " + member);
//                }
//
//                @Override
//                public void done() {
//                    println("Done!");
//                }
//            };
//            executorService.execute(echoTask);
//            Object result = echoTask.get();
//            println(result);
//        } catch (Exception e) {
//            e.printStackTrace();
//        }
//    }

    public void entryAdded(EntryEvent event) {
        println(event);
    }

    public void entryRemoved(EntryEvent event) {
        println(event);
    }

    public void entryUpdated(EntryEvent event) {
        println(event);
    }

    public void entryEvicted(EntryEvent event) {
        println(event);
    }

    public void itemAdded(ItemEvent itemEvent) {
        println("Item added = " + itemEvent.getItem());
    }

    public void itemRemoved(ItemEvent itemEvent) {
        println("Item removed = " + itemEvent.getItem());
    }

    public void onMessage(Message msg) {
        println("Topic received = " + msg.getMessageObject());
    }

    public static class LongTask extends HazelcastInstanceAwareObject implements Callable<String>, Serializable {
        String input = null;

        public LongTask() {
            super();
        }

        public LongTask(String input) {
            super();
            this.input = input;
        }

        public String call() {
            try {
                Thread.sleep(5000);
            } catch (InterruptedException e) {
                System.out.println("Interrupted! Cancelling task!");
                return "No-result";
            }
            return getHazelcastInstance().getCluster().getLocalMember().toString() + ":" + input;
        }
    }

    public static class Echo extends HazelcastInstanceAwareObject implements Callable<String>, Serializable {
        String input = null;

        public Echo() {
            super();
        }

        public Echo(String input) {
            super();
            this.input = input;
        }

        public String call() {
            return getHazelcastInstance().getCluster().getLocalMember().toString() + ":" + input;
        }
    }

    private static class HazelcastInstanceAwareObject implements HazelcastInstanceAware {

        HazelcastInstance hazelcastInstance;

        public HazelcastInstance getHazelcastInstance() {
            return hazelcastInstance;
        }

        public void setHazelcastInstance(HazelcastInstance hazelcastInstance) {
            this.hazelcastInstance = hazelcastInstance;
        }
    }

    protected void handleHelp(String command) {
        boolean silentBefore = silent;
        silent = false;
        println("Commands:");
        println("-- General commands");
        println("echo true|false                      //turns on/off echo of commands (default false)");
        println("silent true|false                    //turns on/off silent of command output (default false)");
        println("#<number> <command>                  //repeats <number> time <command>, replace $i in <command> with current iteration (0..<number-1>)");
        println("&<number> <command>                  //forks <number> threads to execute <command>, replace $t in <command> with current thread number (0..<number-1>");
        println("     When using #x or &x, is is advised to use silent true as well.");
        println("     When using &x with m.putmany and m.removemany, each thread will get a different share of keys unless a start key index is specified");
        println("jvm                                  //displays info about the runtime");
        println("who                                  //displays info about the cluster");
        println("whoami                               //displays info about this cluster member");
        println("ns <string>                          //switch the namespace for using the distributed queue/map/set/list <string> (defaults to \"default\"");
        println("@<file>                              //executes the given <file> script. Use '//' for comments in the script");
        println("");
        println("-- Queue commands");
        println("q.offer <string>                     //adds a string object to the queue");
        println("q.poll                               //takes an object from the queue");
        println("q.offermany <number> [<size>]        //adds indicated number of string objects to the queue ('obj<i>' or byte[<size>]) ");
        println("q.pollmany <number>                  //takes indicated number of objects from the queue");
        println("q.iterator [remove]                  //iterates the queue, remove if specified");
        println("q.size                               //size of the queue");
        println("q.clear                              //clears the queue");
        println("");
        println("-- Set commands");
        println("s.add <string>                       //adds a string object to the set");
        println("s.remove <string>                    //removes the string object from the set");
        println("s.addmany <number>                   //adds indicated number of string objects to the set ('obj<i>')");
        println("s.removemany <number>                //takes indicated number of objects from the set");
        println("s.iterator [remove]                  //iterates the set, removes if specified");
        println("s.size                               //size of the set");
        println("s.clear                              //clears the set");
        println("");
        println("-- Lock commands");
        println("lock <key>                           //same as Hazelcast.getLock(key).lock()");
        println("tryLock <key>                        //same as Hazelcast.getLock(key).tryLock()");
        println("tryLock <key> <time>                 //same as tryLock <key> with timeout in seconds");
        println("unlock <key>                         //same as Hazelcast.getLock(key).unlock()");
        println("");
        println("-- Map commands");
        println("m.put <key> <value>                  //puts an entry to the map");
        println("m.remove <key>                       //removes the entry of given key from the map");
        println("m.get <key>                          //returns the value of given key from the map");
        println("m.putmany <number> [<size>] [<index>]//puts indicated number of entries to the map ('key<i>':byte[<size>], <index>+(0..<number>)");
        println("m.removemany <number> [<index>]      //removes indicated number of entries from the map ('key<i>', <index>+(0..<number>)");
        println("     When using &x with m.putmany and m.removemany, each thread will get a different share of keys unless a start key <index> is specified");
        println("m.keys                               //iterates the keys of the map");
        println("m.values                             //iterates the values of the map");
        println("m.entries                            //iterates the entries of the map");
        println("m.iterator [remove]                  //iterates the keys of the map, remove if specified");
        println("m.size                               //size of the map");
        println("m.clear                              //clears the map");
        println("m.destroy                            //destroys the map");
        println("m.lock <key>                         //locks the key");
        println("m.tryLock <key>                      //tries to lock the key and returns immediately");
        println("m.tryLock <key> <time>               //tries to lock the key within given seconds");
        println("m.unlock <key>                       //unlocks the key");
        println("");
        println("-- List commands:");
        println("l.add <string>");
        println("l.add <index> <string>");
        println("l.contains <string>");
        println("l.remove <string>");
        println("l.remove <index>");
        println("l.set <index> <string>");
        println("l.iterator [remove]");
        println("l.size");
        println("l.clear");
        print("");
        println("-- IAtomicLong commands:");
        println("a.get");
        println("a.set <long>");
        println("a.inc");
        println("a.dec");
        print("");
        println("-- Executor Service commands:");
        println("execute	<echo-input>				//executes an echo task on random member");
        println("execute0nKey	<echo-input> <key>		//executes an echo task on the member that owns the given key");
        println("execute0nMember <echo-input> <key>	//executes an echo task on the member with given index");
        println("execute0nMembers <echo-input> 		//executes an echo task on all of the members");
        println("");
        silent = silentBefore;
    }

    public void println(Object obj) {
        if (!silent)
            System.out.println(obj);
    }

    public void print(Object obj) {
        if (!silent)
            System.out.print(obj);
    }
}
