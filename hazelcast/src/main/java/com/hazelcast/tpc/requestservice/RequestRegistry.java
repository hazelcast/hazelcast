package com.hazelcast.tpc.requestservice;

import com.hazelcast.tpc.engine.frame.Frame;
import org.jctools.util.PaddedAtomicLong;

import java.net.SocketAddress;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;

import static java.lang.System.currentTimeMillis;

class RequestRegistry {

    private final int concurrentRequestLimit;
    private final long nextCallIdTimeoutMs = TimeUnit.SECONDS.toMillis(10);
    private final ConcurrentMap<SocketAddress, Requests> requestsPerSocket = new ConcurrentHashMap<>();

    public RequestRegistry(int concurrentRequestLimit) {
        this.concurrentRequestLimit = concurrentRequestLimit;
    }

    void shutdown(){
        for (Requests requests : requestsPerSocket.values()) {
            for (Frame request : requests.map.values()) {
                request.future.completeExceptionally(new RuntimeException("Shutting down"));
            }
        }
    }

    Requests get(SocketAddress address){
        return requestsPerSocket.get(address);
    }

    Requests getRequestsOrCreate(SocketAddress address) {
        Requests requests = requestsPerSocket.get(address);
        if (requests == null) {
            Requests newRequests = new Requests();
            Requests foundRequests = requestsPerSocket.putIfAbsent(address, newRequests);
            return foundRequests == null ? newRequests : foundRequests;
        } else {
            return requests;
        }
    }

    class Requests {
        final ConcurrentMap<Long, Frame> map = new ConcurrentHashMap<>();
        final PaddedAtomicLong started = new PaddedAtomicLong();
        final PaddedAtomicLong completed = new PaddedAtomicLong();

        void complete() {
            if (concurrentRequestLimit > -1) {
                completed.incrementAndGet();
            }
        }

        long nextCallId() {
            if (concurrentRequestLimit == -1) {
                return started.incrementAndGet();
            } else {
                long endTime = currentTimeMillis() + nextCallIdTimeoutMs;
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
                } while (currentTimeMillis() < endTime);

                throw new RuntimeException("Member is overloaded with requests");
            }
        }

        long nextCallId(int count) {
            if (concurrentRequestLimit == -1) {
                return started.addAndGet(count);
            } else {
                long endTime = currentTimeMillis() + nextCallIdTimeoutMs;
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
                } while (currentTimeMillis() < endTime);

                throw new RuntimeException("Member is overloaded with requests");
            }
        }
    }
}
