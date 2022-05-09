package com.hazelcast.tpc.requestservice;

import com.hazelcast.tpc.engine.frame.Frame;
import jnr.constants.platform.Sock;
import org.jctools.util.PaddedAtomicLong;

import java.net.SocketAddress;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;

class RequestRegistry {

    private final int concurrentRequestLimit;

    public RequestRegistry(int concurrentRequestLimit) {
        this.concurrentRequestLimit = concurrentRequestLimit;
    }

    private final ConcurrentMap<SocketAddress, Requests> requestsPerChannel = new ConcurrentHashMap<>();

    void shutdown(){
        for (Requests requests : requestsPerChannel.values()) {
            for (Frame request : requests.map.values()) {
                request.future.completeExceptionally(new RuntimeException("Shutting down"));
            }
        }
    }

    Requests get(SocketAddress address){
        return requestsPerChannel.get(address);
    }

    Requests getRequestsOrCreate(SocketAddress address) {
        Requests requests = requestsPerChannel.get(address);
        if (requests == null) {
            Requests newRequests = new Requests();
            Requests foundRequests = requestsPerChannel.putIfAbsent(address, newRequests);
            return foundRequests == null ? newRequests : foundRequests;
        } else {
            return requests;
        }
    }

    class Requests {
        final ConcurrentMap<Long, Frame> map = new ConcurrentHashMap<>();
        final PaddedAtomicLong started = new PaddedAtomicLong();
        final PaddedAtomicLong completed = new PaddedAtomicLong();

        public void complete() {
            if (concurrentRequestLimit > -1) {
                completed.incrementAndGet();
            }
        }

        public long nextCallId() {
            if (concurrentRequestLimit == -1) {
                return started.incrementAndGet();
            } else {
                long endTime = System.currentTimeMillis() + TimeUnit.SECONDS.toMillis(10);
                do {
                    if (completed.get() + concurrentRequestLimit > started.get()) {
                        return started.incrementAndGet();
                    } else {
                        try {
                            Thread.sleep(100);
                        } catch (InterruptedException e) {
                            throw new RuntimeException();
                        }
                    }
                } while (System.currentTimeMillis() < endTime);

                throw new RuntimeException("Member is overloaded with requests");
            }
        }

        public long nextCallId(int count) {
            if (concurrentRequestLimit == -1) {
                return started.addAndGet(count);
            } else {
                long endTime = System.currentTimeMillis() + TimeUnit.SECONDS.toMillis(10);
                do {
                    if (completed.get() + concurrentRequestLimit > started.get() + count) {
                        return started.addAndGet(count);
                    } else {
                        try {
                            Thread.sleep(100);
                        } catch (InterruptedException e) {
                            throw new RuntimeException();
                        }
                    }
                } while (System.currentTimeMillis() < endTime);

                throw new RuntimeException("Member is overloaded with requests");
            }
        }
    }
}
