package com.hazelcast.collection.impl.queue;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import com.hazelcast.collection.impl.queue.duplicate.PriorityElementTaskQueue;
import com.hazelcast.collection.impl.queue.duplicate.PriorityElementTaskQueueImpl;
import com.hazelcast.collection.impl.queue.model.PriorityElement;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;

@Category({QuickTest.class, ParallelJVMTest.class})
public class QueuePriorityWithDuplicateCheckTest {

	private static PriorityElementTaskQueue queue;

	@Before
	public void before() {
		queue = new PriorityElementTaskQueueImpl();
	}

	@Test
	public void queue() {
		final PriorityElement element = new PriorityElement(false, 1);
		queue.enqueue(element);
		assertEquals(element, queue.dequeue());
		assertNull(queue.dequeue());
	}

	@Test
	public void queueDouble() {
		final PriorityElement element = new PriorityElement(false, 1);
		queue.enqueue(element);
		queue.enqueue(element);
		queue.enqueue(element);
		assertEquals(element, queue.dequeue());
		assertNull(queue.dequeue());
	}

	@Test
	public void queuePriorizing() {
		testWithSize(800);
	}

	private void testWithSize(final int size) {
		int count = 0;
		for (int i = 0; i < size; i++) {
			queue.enqueue(new PriorityElement(false, count));
			queue.enqueue(new PriorityElement(true, count));
			count++;
		}
		for (int i = 0; i < size; i++) {
			final PriorityElement dequeue = queue.dequeue();
			assertTrue("Highpriority first", dequeue.isHighPriority());
		}
		for (int i = 0; i < size; i++) {
			final PriorityElement dequeue = queue.dequeue();
			assertFalse("Lowpriority afterwards", dequeue.isHighPriority());
		}
		assertNull(queue.dequeue());
	}

	@Test
	public void size02000() {
		testWithSize(2000);
	}

	@Test
	public void size04000() {
		testWithSize(4000);
	}

	@Test
	public void size08000() {
		testWithSize(8000);
	}

	@Test
	public void size16000() {
		testWithSize(16000);
	}

	@Test
	public void size32000() {
		testWithSize(32000);
	}

	@Test
	public void size64000() {
		testWithSize(64000);
	}

	@Test
	public void queueConsistency() throws InterruptedException {
		int count = 0;
		for (int i = 0; i < 500; i++) {
			queue.enqueue(new PriorityElement(false, count));
			queue.enqueue(new PriorityElement(true, count));
			count++;
		}
		final ExecutorService threadPool = Executors.newCachedThreadPool();
		final ConcurrentSkipListSet<PriorityElement> tasks = new ConcurrentSkipListSet<>();
		final Semaphore sem = new Semaphore(-99);
		for (int i = 0; i < 100; i++) {
			threadPool.execute(new Runnable() {

				@Override
				public void run() {
					PriorityElement task;
					while ((task = queue.dequeue()) != null) {
						tasks.add(task);
					}
					sem.release();
				}
			});
		}
		sem.acquire();
		assertEquals(500 * 2, tasks.size());
	}

	@Test
	public void queueParallel() throws InterruptedException {
		final AtomicInteger enqueued = new AtomicInteger();
		final AtomicInteger dequeued = new AtomicInteger();
		final ExecutorService threadPool = Executors.newCachedThreadPool();
		final Semaphore sem = new Semaphore(-200);
		final int size = 1000;
		for (int i = 0; i <= 100; i++) {
			threadPool.execute(new Runnable() {

				@Override
				public void run() {
					while (enqueued.get() < size) {
						final int j = enqueued.incrementAndGet();
						final boolean prioriy = j % 2 == 0;
						final PriorityElement task = new PriorityElement(prioriy, j);
						queue.enqueue(task);
					}
					sem.release();
				}
			});
			threadPool.execute(new Runnable() {

				@Override
				public void run() {
					while (enqueued.get() > dequeued.get() || enqueued.get() < size) {
						final PriorityElement dequeue = queue.dequeue();
						if (dequeue != null) {
							dequeued.incrementAndGet();
						}
					}
					sem.release();
				}
			});
		}
		sem.acquire();
		assertEquals(enqueued.get(), dequeued.get());
		assertNull(queue.dequeue());
	}

	@Test
	public void offer_poll_and_offer_poll_again() {
		PriorityElement task = new PriorityElement(false, 1);

		assertEquals(null, queue.dequeue());
		assertTrue(queue.enqueue(task));
		assertEquals(task, queue.dequeue());
		assertEquals(null, queue.dequeue());
		assertTrue(queue.enqueue(task));
		assertEquals(task, queue.dequeue());
	}
}