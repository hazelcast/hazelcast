package com.hazelcast.spi.impl.nextgen;

import com.hazelcast.cluster.Address;
import com.hazelcast.instance.EndpointQualifier;
import com.hazelcast.internal.nio.Packet;
import com.hazelcast.internal.serialization.InternalSerializationService;
import com.hazelcast.internal.serialization.SerializationService;
import com.hazelcast.internal.serialization.impl.ByteArrayObjectDataInput;
import com.hazelcast.internal.server.ServerConnectionManager;
import com.hazelcast.internal.util.HashUtil;
import com.hazelcast.logging.ILogger;
import com.hazelcast.spi.impl.NodeEngineImpl;
import com.hazelcast.table.impl.SelectByKeyOperation;
import com.hazelcast.table.impl.UpsertOperation;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;

import static com.hazelcast.internal.nio.Packet.FLAG_OP_RESPONSE;
import static com.hazelcast.spi.impl.nextgen.Op.RUN_CODE_DONE;
import static com.hazelcast.spi.impl.nextgen.Op.RUN_CODE_FOO;
import static com.hazelcast.spi.impl.nextgen.OpCodes.TABLE_SELECT_BY_KEY;
import static com.hazelcast.spi.impl.nextgen.OpCodes.TABLE_UPSERT;

public class OpService implements Consumer<Packet> {

    private final NodeEngineImpl nodeEngine;
    private final SerializationService ss;
    private final ILogger logger;
    private final Address thisAddress;
    private volatile ServerConnectionManager connectionManager;
    private volatile boolean shuttingdown = false;
    private final RequestThread[] requestThreads;
    private final Managers managers = new Managers();

    public OpService(NodeEngineImpl nodeEngine) {
        this.nodeEngine = nodeEngine;
        this.logger = nodeEngine.getLogger(OpService.class);
        this.ss = nodeEngine.getSerializationService();
        this.requestThreads = new RequestThread[8];
        this.thisAddress = nodeEngine.getThisAddress();
        for (int k = 0; k < requestThreads.length; k++) {
            requestThreads[k] = new RequestThread();
        }
    }

    public void start() {
        logger.finest("Starting Nextgen");

        for (RequestThread t : requestThreads) {
            t.start();
        }
    }

    public void shutdown() {
        shuttingdown = true;
    }

    public class Invocation{
        public long callId;
        public Request request;
        private CompletableFuture completableFuture = new CompletableFuture();
    }

    public CompletableFuture invoke(Request request) {
        int partitionId = request.partitionId;

        if (partitionId >= 0) {
            Address targetAddress = nodeEngine.getPartitionService().getPartitionOwner(partitionId);
            TargetInvocations invocations = getTargetInvocations(targetAddress);
            Invocation invocation = new Invocation();
            invocation.callId = invocations.counter.incrementAndGet();
            invocation.request = request;
            invocations.map.put(invocation.callId, invocation);

            if (targetAddress.equals(thisAddress)) {
                accept(request);
            } else {
                if(connectionManager == null)
                    connectionManager = nodeEngine.getNode().getServer().getConnectionManager(EndpointQualifier.MEMBER);

                connectionManager.transmit(request.toPacket(), targetAddress);
            }

            return invocation.completableFuture;
        } else {
            throw new RuntimeException();
        }
    }

    public TargetInvocations getTargetInvocations(Address address) {
        TargetInvocations invocations = invocationsPerMember.get(address);
        if (invocations != null) {
            return invocations;
        }

        TargetInvocations newInvocations = new TargetInvocations(address);
        TargetInvocations foundInvocations = invocationsPerMember.putIfAbsent(address, newInvocations);
        return foundInvocations == null ? newInvocations : foundInvocations;
    }

    private final ConcurrentMap<Address, TargetInvocations> invocationsPerMember = new ConcurrentHashMap<>();

    private class TargetInvocations {
        private final Address target;
        private final ConcurrentMap<Long, Invocation> map = new ConcurrentHashMap<>();
        private final AtomicLong counter = new AtomicLong(0);

        public TargetInvocations(Address target) {
            this.target = target;
        }

    }

    public void accept(Request request) {
        int partitionId = request.partitionId;
        if (partitionId < 0) {
            throw new RuntimeException();
        }
        int index = HashUtil.hashToIndex(partitionId, requestThreads.length);
        requestThreads[index].engine.taskQueue.add(request);
    }

    //todo: add option to bypass offloading to thread for thread per core versionn
    @Override
    public void accept(Packet packet) {
        if(packet.isFlagRaised(FLAG_OP_RESPONSE)){
            Address remoteAddress = packet.getConn().getRemoteAddress();
            TargetInvocations targetInvocations = invocationsPerMember.get(remoteAddress);
            if(targetInvocations == null){
                System.out.println("Dropping response "+packet+", targetInvocations not found");
                return;
            }

            long callId = 0;
            Invocation invocation = targetInvocations.map.get(callId);
            if(invocation == null){
                System.out.println("Dropping response "+packet+", invocation not found");
                invocation.completableFuture.complete(null);
            }
        }else{
            int index = HashUtil.hashToIndex(packet.getPartitionHash(), requestThreads.length);
            requestThreads[index].engine.taskQueue.add(packet);
        }
    }

    class Engine {

        private final BlockingQueue taskQueue = new LinkedBlockingQueue();

        public void tick() throws InterruptedException {
            Object item = taskQueue.take();
            if (item instanceof Packet) {
                run((Packet) item);
            } else if (item instanceof Op) {
                run((Op) item);
            } else if ((item instanceof Request)) {
                run((Request) item);
            }
        }

        private void run(Packet packet) {
            try {
                byte[] bytes = packet.toByteArray();
                byte opcode = bytes[0];
                Op op = newOp(opcode);
                op.in.init(packet.toByteArray(), 1);
                op.managers = managers;
                run(op);
            } catch (Exception e) {
                e.printStackTrace();
            }
        }

        private void run(Request request) {
            throw new RuntimeException();
//            try {
//                byte[] bytes = packet.toByteArray();
//                byte opcode = request.opcode;
//                Op op = newOp(opcode);
//                op.in.init(packet.toByteArray(), 1);
//                op.managers = managers;
//                run(op);
//            } catch (Exception e) {
//                e.printStackTrace();
//            }
        }

        private void run(Op op) {
            try {
                int runCode = op.run();
                switch (runCode) {
                    case RUN_CODE_DONE:
                        free(op);
                        return;
                    case RUN_CODE_FOO:
                        throw new RuntimeException();
                    default:
                        throw new RuntimeException();
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
        }

        private Op newOp(int opcode) {
            Op op;
            switch (opcode) {
                case TABLE_UPSERT:
                    op = new UpsertOperation();
                    break;
                case TABLE_SELECT_BY_KEY:
                    op = new SelectByKeyOperation();
                    break;
                default:
                    throw new RuntimeException("Unrecognized opcode:" + opcode);
            }
            op.in = new ByteArrayObjectDataInput(null, (InternalSerializationService) ss, null);
            return op;
        }

        private void free(Op op) {
            op.cleanup();

            //we should return it to the pool.
        }
    }

    class RequestThread extends Thread {
        private final Engine engine = new Engine();

        public void run() {
            try {
                loop();
            } catch (Exception e) {
                e.printStackTrace();
            }
        }

        private void loop() throws InterruptedException {
            while (!shuttingdown) {
                engine.tick();
            }
        }
    }
}
