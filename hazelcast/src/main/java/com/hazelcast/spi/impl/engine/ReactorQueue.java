package com.hazelcast.spi.impl.engine;

import com.hazelcast.internal.util.QuickMath;
import org.jctools.util.PaddedAtomicLong;

import java.util.Collection;

import static org.jctools.util.UnsafeRefArrayAccess.calcCircularRefElementOffset;
import static org.jctools.util.UnsafeRefArrayAccess.lvRefElement;
import static org.jctools.util.UnsafeRefArrayAccess.soRefElement;

public class ReactorQueue {

    // These are all local to the ReactorThread
    // The head_block contains 2 pieces of info; the sequence and if the listener is blocked.
    public final PaddedAtomicLong head_block = new PaddedAtomicLong(1);
    public final PaddedAtomicLong dirtyHead = new PaddedAtomicLong(1);
    public final PaddedAtomicLong cachedTail = new PaddedAtomicLong(0);

    // Accessed by the producer threads.
    public final PaddedAtomicLong tail = new PaddedAtomicLong(0);

    private final Object[] array;
    private final int mask;
    private final int capacity;

    public ReactorQueue(int capacity) {
        capacity = QuickMath.nextPowerOfTwo(capacity) * 8;
        this.array = new Object[capacity];
        this.capacity = capacity;
        this.mask = capacity - 1;
    }

    public int dirtySize() {
        return (int) (tail.get() - dirtyHead.get() + 1);
    }

    public int size() {
        long head_block = this.head_block.get();
        long head = head_block < 0 ? -head_block : head_block;
        return (int) (tail.get() - head + 1);
    }

    public void commit() {
        if (dirtyHead.get() < 0) {
            throw new RuntimeException();
        }

        //System.out.println("commit     " + dirtyHead.get());
        head_block.set(dirtyHead.get());
    }

    /**
     * @return true if the thread can continue blocking, false otherwise.
     */
    public boolean commitAndMarkBlocked() {
        long dirtyHead = this.dirtyHead.get();
        if (dirtyHead < 0) {
            throw new RuntimeException();
        }

        long currentTail = tail.get();
        // lets cache the tail since we have it
        cachedTail.lazySet(currentTail);

        if (currentTail < dirtyHead) {
            // The queue was empty
            // The head_block is set to a negative value to indicate that the
            // consumer thread is going to block and needs to be notified.
            head_block.set(-dirtyHead);

            currentTail = tail.get();
            // lets cache the tail since we have it
            cachedTail.lazySet(currentTail);

            // we need to re-check if any work was found.
            if (currentTail < dirtyHead) {
                // The queue is still empty.
                //System.out.println("commitAndMarkBlocked     block=true dirtySize:" + dirtySize() + " size:" + size() + " dirty_head:" + dirtyHead + " tail:" + tail.get());

                // The consumer thread can now continue with the blocking wait.
                return true;
            }
        }

        // The queue was not empty
        // By setting to head_block to a positive value, we signal that
        // the consumer thread is not going to block on a wait.
        head_block.set(dirtyHead);
        //System.out.println("commitAndMarkBlocked     block=false");
        return false;
    }

    public void markAwake() {
        head_block.set(dirtyHead.get());
       // System.out.println("Mark awake     : " + head_block);
    }

    /**
     * Adds an item and checks if the consumer was blocked.
     *
     * @param item
     * @return true if the consumer was blocked and hence wakup is needed, false otherwise.
     */
    public boolean addAndCheckBlocked(Object item) {
        //System.out.println("addAndMarkedBlocked----------------");
        long newTail;
        long oldTail;
        long head_block;
        long head;
        boolean signal;
        int oldSize;
        for (; ; ) {
            oldTail = tail.get();
            newTail = oldTail + 1;

           // System.out.println("addAndMarkedBlocked     oldTail:" + oldTail);
           // System.out.println("addAndMarkedBlocked     newTail:" + newTail);

            head_block = this.head_block.get();
            head = head_block < 0 ? -head_block : head_block;
            oldSize = (int) (oldTail - head + 1);
           // System.out.println("addAndMarkedBlocked     oldSize:" + oldSize);
           // System.out.println("addAndMarkedBlocked     head_block:" + head_block);


//            System.out.println("old size: "+(oldTail-head+1));
//            System.out.println("new size: "+(newTail-head+1));

            if (oldTail - head + 1 == capacity) {
                // queue is full.
                throw new RuntimeException("Queue is full");
            } else if (oldTail < head) {
                // the item is added in an empty queue, so we need to check head if the consumer is blocked.
                signal = head_block < 0;
            //    System.out.println("addAndMarkedBlocked     Queue was empty");
            } else {
                // the queue was not empty, so signal not needed because somebody else
                // already must have taken care of it.
                signal = false;
              //  System.out.println("addAndMarkedBlocked     Queue was not empty");
            }

            if (tail.compareAndSet(oldTail, newTail)) {
                // todo: this should not be needed. It is task of the commitAndMarkBlocked to ensure that it
                // isn't going to block even though there is pending work.
                if (!signal) {
                    long head_block2 = this.head_block.get();
                    if (head_block2 != head_block) {
                        long head2 = head_block2 < 0 ? -head_block2 : head_block2;
                        if (oldTail < head2) {
                            // the item is added in an empty queue, so we need to check head if the consumer is blocked.
                            signal = head_block2 < 0;
                          //  System.out.println("addAndMarkedBlocked     Queue was empty");
                        } else {
                            // the queue was not empty, so signal not needed because somebody else
                            // already must have taken care of it.
                            signal = false;
                           // System.out.println("addAndMarkedBlocked     Queue was not empty");
                        }
                    }
                }
                break;
            }
        }
//
//        System.out.println("addAndMarkedBlocked     signal:" + signal);
//        System.out.println("addAndMarkedBlocked     dirtyHead:" + dirtyHead.get());
//        System.out.println("addAndMarkedBlocked     head_block:" + this.head_block.get());
//        System.out.println("addAndMarkedBlocked     tail:" + this.tail.get());
//        System.out.println("addAndMarkedBlocked     cached_tail:" + this.cachedTail.get());
//        System.out.println("addAndMarkedBlocked     size:" + size());
//        System.out.println("addAndMarkedBlocked     oldSize:" + oldSize);

        long offset = toIndex(newTail);
        soRefElement(array, offset, item);
        return signal;
    }

    public boolean addAndCheckBlocked(Collection items) {
        throw new RuntimeException();
    }

    public static long encode(long seq, boolean blocked) {
        return blocked ? -seq : seq;
    }

    public Object poll() {
        if (cachedTail.get() < dirtyHead.get()) {
            cachedTail.lazySet(tail.get());
        }

        if (cachedTail.get() < dirtyHead.get()) {
            return null;
        }

        long h = dirtyHead.get();
        long offset = toIndex(h);
        Object item;
        do {
            item = lvRefElement(array, offset);
        } while (item == null);

        soRefElement(array, offset, null);

        this.dirtyHead.lazySet(h + 1);
        return item;
    }

    private long toIndex(long h) {
        return calcCircularRefElementOffset(h * 8, mask);
    }
}
