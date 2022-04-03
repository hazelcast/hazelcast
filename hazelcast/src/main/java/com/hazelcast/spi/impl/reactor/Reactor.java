package com.hazelcast.spi.impl.reactor;


import com.hazelcast.cluster.Address;
import com.hazelcast.internal.nio.Connection;
import com.hazelcast.internal.util.executor.HazelcastManagedThread;
import com.hazelcast.logging.ILogger;
import com.hazelcast.table.impl.NoOp;
import com.hazelcast.table.impl.SelectByKeyOperation;
import com.hazelcast.table.impl.UpsertOp;

import java.net.SocketAddress;
import java.util.Collection;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArraySet;
import java.util.concurrent.Future;

import static com.hazelcast.spi.impl.reactor.Op.RUN_CODE_DONE;
import static com.hazelcast.spi.impl.reactor.Op.RUN_CODE_FOO;
import static com.hazelcast.spi.impl.reactor.OpCodes.TABLE_NOOP;
import static com.hazelcast.spi.impl.reactor.OpCodes.TABLE_SELECT_BY_KEY;
import static com.hazelcast.spi.impl.reactor.OpCodes.TABLE_UPSERT;

public abstract class Reactor extends HazelcastManagedThread {
    protected final ReactorFrontEnd frontend;
    protected final ILogger logger;
    protected final Address thisAddress;
    protected final int port;
    protected final ChannelConfig channelConfig;
    protected final Set<Channel> channels = new CopyOnWriteArraySet<>();

    public Reactor(ReactorFrontEnd frontend, ChannelConfig channelConfig, Address thisAddress, int port, String name) {
        super(name);
        this.frontend = frontend;
        this.channelConfig = channelConfig;
        this.logger = frontend.nodeEngine.getLogger(getClass());
        this.thisAddress = thisAddress;
        this.port = port;
    }

    public Future<Channel> schedule(SocketAddress address, Connection connection) {
        System.out.println("asyncConnect connect to " + address);

        ConnectRequest request = new ConnectRequest();
        request.address = address;
        request.connection = connection;
        request.future = new CompletableFuture<>();

        schedule(request);

        return request.future;
    }

    protected abstract void schedule(ConnectRequest request);

    public static class ConnectRequest {
        public Connection connection;
        public SocketAddress address;
        public CompletableFuture<Channel> future;
    }

    @Override
    public final void executeRun() {
        try {
            setupServerSocket();
        } catch (Throwable e) {
            logger.severe(e);
            return;
        }

        try {
            eventLoop();
        } catch (Throwable e) {
            e.printStackTrace();
            logger.severe(e);
        }
    }


    protected abstract void setupServerSocket() throws Exception;

    protected abstract void eventLoop() throws Exception;

    /**
     * Is called for local requests.
     *
     * @param request
     */
    public abstract void schedule(Invocation request);

    public Collection<Channel> channels() {
        return channels;
    }

    protected void handleRemoteOp(Frame request) {
        try {
            Frame response = handleOp(request);
            request.channel.write(response);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    // local call
    protected void handleLocalOp(Invocation inv) {
        try {
            Frame response = handleOp(inv.request);
            inv.future.complete(response);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    protected Frame handleOp(Frame request) throws Exception {
        Op op = allocateOp(request);

        int runCode = op.run();
        try {
            switch (runCode) {
                case RUN_CODE_DONE:
                    return op.response;
                case RUN_CODE_FOO:
                    throw new RuntimeException();
                default:
                    throw new RuntimeException();
            }
        } finally {
            free(op);
        }
    }

    // Hacky caching.
    private UpsertOp upsertOp;
    private NoOp noOp;
    private SelectByKeyOperation selectByKeyOperation;

    protected final Op allocateOp(Frame request) {
        int opcode = request.getInt(Frame.OFFSET_REQUEST_OPCODE);
        Op op;
        switch (opcode) {
            case TABLE_UPSERT:
                if (upsertOp == null) {
                    upsertOp = new UpsertOp();
                    upsertOp.response = new Frame();
                    upsertOp.managers = frontend.managers;
                }
                op = upsertOp;
                break;
            case TABLE_SELECT_BY_KEY:
                if (selectByKeyOperation == null) {
                    selectByKeyOperation = new SelectByKeyOperation();
                    selectByKeyOperation.response = new Frame();
                    selectByKeyOperation.managers = frontend.managers;
                }
                op = selectByKeyOperation;
                break;
            case TABLE_NOOP:
                if (noOp == null) {
                    noOp = new NoOp();
                    noOp.response = new Frame();
                    noOp.managers = frontend.managers;
                }
                op = noOp;
                break;
            default:
                throw new RuntimeException("Unrecognized opcode:" + opcode);
        }
        op.request = request;
        op.request.position(Frame.OFFSET_REQUEST_PAYLOAD);
        op.response = new Frame(20);
        return op;
    }

    private void free(Op op) {
        op.cleanup();
        op.request = null;
        op.response = null;

        //we should return it to the pool.
    }
}
