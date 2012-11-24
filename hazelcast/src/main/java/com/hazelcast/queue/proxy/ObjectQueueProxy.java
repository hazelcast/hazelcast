package com.hazelcast.queue.proxy;

import com.hazelcast.core.Instance;
import com.hazelcast.core.ItemListener;
import com.hazelcast.monitor.LocalQueueStats;
import com.hazelcast.nio.Data;
import com.hazelcast.queue.QueueService;
import com.hazelcast.spi.NodeService;

import java.util.Collection;
import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.concurrent.TimeUnit;

import static com.hazelcast.nio.IOUtil.toObject;

/**
 * Created with IntelliJ IDEA.
 * User: ali
 * Date: 11/14/12
 * Time: 13:23 AM
 * To change this template use File | Settings | File Templates.
 */
public class ObjectQueueProxy<E> extends QueueProxySupport implements QueueProxy<E> {

    public ObjectQueueProxy(String name, QueueService queueService, NodeService nodeService) {
        super(name, queueService, nodeService);
    }

    public LocalQueueStats getLocalQueueStats() {
        return null;  //To change body of implemented methods use File | Settings | File Templates.
    }

    public boolean add(E e) {
        boolean res = offer(e);
        if(!res){
            throw new IllegalStateException();
        }
        return res;  //To change body of implemented methods use File | Settings | File Templates.
    }

    public boolean offer(E e) {
        try {
            return offer(e,-1,null);
        } catch (InterruptedException ex) {
            ex.printStackTrace();
        }
        return false;  //To change body of implemented methods use File | Settings | File Templates.
    }

    public void put(E e) throws InterruptedException {
        //To change body of implemented methods use File | Settings | File Templates.
    }

    public boolean offer(E e, long ttl, TimeUnit timeUnit) throws InterruptedException {
        Data data = nodeService.toData(e);
        return offerInternal(data,ttl, timeUnit);
    }

    public E take() throws InterruptedException {
        return null;  //To change body of implemented methods use File | Settings | File Templates.
    }

    public E poll(long l, TimeUnit timeUnit) throws InterruptedException {
        return null;  //To change body of implemented methods use File | Settings | File Templates.
    }

    public int remainingCapacity() {
        return 0;  //To change body of implemented methods use File | Settings | File Templates.
    }

    public boolean remove(Object o) {
        return false;  //To change body of implemented methods use File | Settings | File Templates.
    }

    public boolean contains(Object o) {
        return false;  //To change body of implemented methods use File | Settings | File Templates.
    }

    public int drainTo(Collection<? super E> objects) {
        return 0;  //To change body of implemented methods use File | Settings | File Templates.
    }

    public int drainTo(Collection<? super E> objects, int i) {
        return 0;  //To change body of implemented methods use File | Settings | File Templates.
    }

    public E remove() {
        E res = poll();
        if(res == null){
            throw new NoSuchElementException();
        }
        return res;
    }

    public E poll() {
        Data data = peekInternal(true);
        return (E) nodeService.toObject(data);
    }

    public E element() {
        E res = peek();
        if(res == null){
            throw new NoSuchElementException();
        }
        return res;
    }

    public E peek() {
        Data data = peekInternal(false);
        return (E) nodeService.toObject(data);
    }

    public boolean isEmpty() {
        return false;  //To change body of implemented methods use File | Settings | File Templates.
    }

    public Iterator<E> iterator() {
        return null;  //To change body of implemented methods use File | Settings | File Templates.
    }

    public Object[] toArray() {
        return new Object[0];  //To change body of implemented methods use File | Settings | File Templates.
    }

    public <T> T[] toArray(T[] ts) {
        return null;  //To change body of implemented methods use File | Settings | File Templates.
    }

    public boolean containsAll(Collection<?> objects) {
        return false;  //To change body of implemented methods use File | Settings | File Templates.
    }

    public boolean addAll(Collection<? extends E> es) {
        return false;  //To change body of implemented methods use File | Settings | File Templates.
    }

    public boolean removeAll(Collection<?> objects) {
        return false;  //To change body of implemented methods use File | Settings | File Templates.
    }

    public boolean retainAll(Collection<?> objects) {
        return false;  //To change body of implemented methods use File | Settings | File Templates.
    }

    public void clear() {
        //To change body of implemented methods use File | Settings | File Templates.
    }

    public String getName() {
        return null;  //To change body of implemented methods use File | Settings | File Templates.
    }

    public void addItemListener(ItemListener<E> listener, boolean includeValue) {
        //To change body of implemented methods use File | Settings | File Templates.
    }

    public void removeItemListener(ItemListener<E> listener) {
        //To change body of implemented methods use File | Settings | File Templates.
    }

    public InstanceType getInstanceType() {
        return null;  //To change body of implemented methods use File | Settings | File Templates.
    }

    public void destroy() {
        //To change body of implemented methods use File | Settings | File Templates.
    }

    public Object getId() {
        return null;  //To change body of implemented methods use File | Settings | File Templates.
    }
}
