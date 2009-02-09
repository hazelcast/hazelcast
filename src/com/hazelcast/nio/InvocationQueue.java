/* 
 * Copyright (c) 2007-2008, Hazel Ltd. All Rights Reserved.
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
 *
 */

package com.hazelcast.nio;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.nio.ByteBuffer;
import java.security.MessageDigest;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.atomic.AtomicInteger;

import com.hazelcast.impl.Constants;
import com.hazelcast.impl.Node;

public class InvocationQueue {

	private static final int INV_COUNT = 2000;

	BlockingQueue<Invocation> qinvocations = new ArrayBlockingQueue<Invocation>(INV_COUNT);

	private static final InvocationQueue instance = new InvocationQueue();

	private InvocationQueue() {
		try {
			for (int i = 0; i < INV_COUNT; i++) {
				qinvocations.add(new Invocation(this));
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	public static InvocationQueue get() {
		return instance;
	}

	public Invocation obtainInvocation() {
		Invocation inv = null;
		inv = qinvocations.poll();
		if (inv == null) {
			inv = new Invocation(this);
			// throw new RuntimeException(Thread.currentThread().getName() +
			// " Couldn't obtain ServiceInvocation !!");
		}
		inv.reset();
		return inv;
	}

	public void returnInvocation(Invocation inv) throws InterruptedException {
		inv.reset();
		qinvocations.offer(inv);
	}

	public Invocation createNewInvocation() {
		return new Invocation(this);
	}

	private static int listMaxSize = 12;

	private static BlockingQueue<ByteBuffer> bufferq = new ArrayBlockingQueue<ByteBuffer>(10000);
	private static AtomicInteger bufferCounter = new AtomicInteger();

	public ByteBuffer obtainBuffer() {
		ByteBuffer bb = bufferq.poll();
		if (bb == null) {
			bb = ByteBuffer.allocate(1024);
			bufferCounter.incrementAndGet();
		} else
			bb.clear();
		return bb;
	}

	public Data createNewData() {
		return new Data();
	}

	public void purge(Data data) {
		if (data == null)
			throw new RuntimeException("Data cannot be null");
		for (ByteBuffer bb : data.lsData) {
			bufferq.offer(bb);
		}
		data.lsData.clear();
		data.prepareForRead();
	}

	public final class Invocation implements Constants, Constants.ResponseTypes {

		private Serializer serializer = null;

		public String name;

		public int operation = -1;

		public ByteBuffer bbSizes = ByteBuffer.allocate(12);

		public ByteBuffer bbHeader = ByteBuffer.allocate(500);

		public Data key = new Data(this);

		public Data data = new Data(this);

		public DataBufferProvider keyBufferProvider = new DataBufferProvider(key, this);

		public DataBufferProvider dataBufferProvider = new DataBufferProvider(data, this);

		public List<ByteBuffer> lsEmptyBuffers = new ArrayList<ByteBuffer>(listMaxSize);

		public long txnId = -1;

		public int threadId = -1;

		public int lockCount = 0;

		public Address lockAddress = null;

		public long timeout = -1;

		public boolean local = true;

		public int blockId = -1;

		public byte responseType = RESPONSE_NONE;

		public boolean scheduled = false;

		public InvocationQueue container;

		public Object attachment;

		public long longValue = Long.MIN_VALUE;

		public long recordId = -1;

		public long eventId = -1;
		
		public Connection conn; 

		public Invocation(InvocationQueue container) {
			this.container = container;
		}

		public void setData(Data newData) {
			data = newData;
			data.inv = this;
		}

		public void write(ByteBuffer socketBB) {
			socketBB.put(bbSizes.array(), 0, bbSizes.limit());
			socketBB.put(bbHeader.array(), 0, bbHeader.limit());
			if (key.size() > 0)
				key.copyToBuffer(socketBB);
			if (data.size() > 0)
				data.copyToBuffer(socketBB);
		}

		protected void putString(ByteBuffer bb, String str) {
			byte[] bytes = str.getBytes();
			bb.putInt(bytes.length);
			bb.put(bytes);

		}

		protected String getString(ByteBuffer bb) {
			int size = bb.getInt();
			byte[] bytes = new byte[size];
			bb.get(bytes);
			return new String(bytes);
		}

		protected void writeBoolean(ByteBuffer bb, boolean value) {
			bb.put((value) ? (byte) 1 : (byte) 0);
		}

		protected boolean readBoolean(ByteBuffer bb) {
			return (bb.get() == (byte) 1) ? true : false;
		}

		public void write() {
			bbSizes.clear();
			bbHeader.clear();

			bbHeader.putInt(operation);
			bbHeader.putInt(blockId);
			bbHeader.putInt(threadId);
			bbHeader.putInt(lockCount);
			bbHeader.putLong(timeout);
			bbHeader.putLong(txnId);
			bbHeader.putLong(longValue);
			bbHeader.putLong(recordId);
			bbHeader.putLong(eventId);
			bbHeader.put(responseType);
			putString(bbHeader, name);
			boolean fromNull = (lockAddress == null);
			writeBoolean(bbHeader, fromNull);
			if (!fromNull) {
				lockAddress.writeObject(bbHeader);
			}
			bbHeader.flip();

			bbSizes.putInt(bbHeader.limit());
			bbSizes.putInt(key.size);
			bbSizes.putInt(data.size);
			bbSizes.flip(); 
		}

		public void read() {
			operation = bbHeader.getInt();
			blockId = bbHeader.getInt();
			threadId = bbHeader.getInt();
			lockCount = bbHeader.getInt();
			timeout = bbHeader.getLong();
			txnId = bbHeader.getLong();
			longValue = bbHeader.getLong();
			recordId = bbHeader.getLong();
			eventId = bbHeader.getLong();
			responseType = bbHeader.get();
			name = getString(bbHeader); 
			boolean fromNull = readBoolean(bbHeader);
			if (!fromNull) {
				lockAddress = new Address();
				lockAddress.readObject(bbHeader);
			}
		}

		public void reset() {
			name = null;
			operation = -1;
			threadId = -1;
			lockCount = 0;
			lockAddress = null;
			timeout = -1;
			txnId = -1;
			responseType = RESPONSE_NONE;
			local = true;
			scheduled = false;
			blockId = -1;
			longValue = Long.MIN_VALUE;
			recordId = -1;
			eventId = -1;
			bbSizes.clear();
			bbHeader.clear();
			key.setNoData();
			data.setNoData();
			attachment = null;
			conn = null;
		}

		

		@Override
		public String toString() {
			return "Invocation " + operation + " name=" + name + "  local=" + local + "  blockId="
					+ blockId + " data=" + data;
		}

		public void flipBuffers() {
			bbSizes.flip();
			bbHeader.flip();
		}

		private boolean sizeRead = false;

		public boolean read(ByteBuffer bb) {
			while (!sizeRead && bbSizes.hasRemaining()) {
				BufferUtil.copy(bb, bbSizes);
			}
			if (!sizeRead && !bbSizes.hasRemaining()) {
				sizeRead = true;
				bbSizes.flip();
				bbHeader.limit(bbSizes.getInt());
				key.size = bbSizes.getInt();
				data.size = bbSizes.getInt();
			}
			// logger.log(Level.INFO,sizeRead + " size " + bbSizes);
			if (sizeRead) {
				while (bb.hasRemaining() && bbHeader.hasRemaining()) {
					BufferUtil.copy(bb, bbHeader);
				}

				while (bb.hasRemaining() && key.shouldRead()) {
					key.read(bb);
				}

				while (bb.hasRemaining() && data.shouldRead()) {
					data.read(bb);
				}
			}

			if (sizeRead && !bbHeader.hasRemaining() && !key.shouldRead() && !data.shouldRead()) {
				sizeRead = false;
				key.postRead();
				data.postRead();
				return true;
			}
			return false;
		}

		public void returnToContainer() {
			if (container != null) {
				try {
					container.returnInvocation(this);
				} catch (InterruptedException e) {
					Node.get().handleInterruptedException(Thread.currentThread(), e);
				}
			} else
				throw new RuntimeException("Should not return to container");
		}

		public Object getValueObject() {
			return getObject(data, dataBufferProvider);
		}

		public Object getKeyObject() {
			return getObject(key, keyBufferProvider);
		}

		public Object getObject(Data data, BufferProvider bufferProvider) {
			if (data == null || data.size == 0)
				return null;
			if (serializer == null)
				serializer = new Serializer();
			try {
				Object obj = serializer.readObject(bufferProvider);
				return obj;
			} catch (Exception e) {
				e.printStackTrace();
				return null;
			}
		}

		public void writeObject(BufferProvider bufferProvider, Object obj) throws Exception {
			if (serializer == null) {
				serializer = new Serializer();
			}
			serializer.writeObject(bufferProvider, obj);
		}

		public void set(String name, int operation, Object k, Object value) throws Exception {
			this.threadId = Thread.currentThread().hashCode();
			this.name = name;
			this.operation = operation;
			if (serializer == null) {
				serializer = new Serializer();
			}

			if (value != null) {
				if (value instanceof Data) {
					doHardCopy((Data) value, data);
				} else {
					serializer.writeObject(dataBufferProvider, value);
					data.postRead();
				}
			}

			if (k != null) {
				if (k instanceof Data) {
					doHardCopy((Data) k, key);
				} else {
					serializer.writeObject(keyBufferProvider, k);
					key.postRead();
				}
			}
		}

		public class DataBufferProvider implements BufferProvider {
			private Data theData = null;

			private Invocation inv = null;

			public DataBufferProvider(Data theData, Invocation inv) {
				super();
				this.theData = theData;
				this.inv = inv;
			}

			public DataBufferProvider() {
				super();
			}

			public void setData(Data theData) {
				this.theData = theData;
			}

			public void set(Data theData, Invocation inv) {
				this.theData = theData;
				this.inv = inv;
			}

			public void addBuffer(ByteBuffer bb) {
				theData.add(bb);
			}

			public ByteBuffer getBuffer(int index) {
				if (index >= theData.lsData.size())
					return null;
				return theData.lsData.get(index);
			}

			public int size() {
				return theData.size;
			}

			public ByteBuffer takeEmptyBuffer() {
				ByteBuffer empty = (inv == null) ? obtainBuffer() : inv.takeEmptyBuffer();
				if (empty.position() != 0 || empty.limit() != 1024)
					throw new RuntimeException("" + empty);
				return empty;
			}
		}

		public void setFromConnection(Connection conn) {
			this.conn = conn;
			if (lockAddress == null)
				lockAddress = conn.getEndPoint();
		}

		public ByteBuffer takeEmptyBuffer() {
			ByteBuffer bb = null;
			if (lsEmptyBuffers.size() == 0) {
				bufferq.drainTo(lsEmptyBuffers, listMaxSize);
			}
			if (lsEmptyBuffers.size() == 0) {
				bb = ByteBuffer.allocate(1024);
				bufferCounter.incrementAndGet();
			} else {
				bb = lsEmptyBuffers.remove(0);
				bb.clear();
			}
			return bb;
		}

		public void setNoData() {
			setNoData(key);
			setNoData(data);
		}

		public void setNoData(Data data) {
			List<ByteBuffer> lsData = data.lsData;
			while (lsData.size() > 0) {
				ByteBuffer bb = lsData.remove(0);
				addEmpty(bb);
			}
			data.prepareForRead();
		}

		public void addEmpty(ByteBuffer bb) {
			bb.clear();
			bb.limit(0);
			if (lsEmptyBuffers.size() < listMaxSize) {
				lsEmptyBuffers.add(bb);
			} else {
				bufferq.offer(bb);
			}
		}

		public Data doHardCopy(Data from) {
			if (from == null || from.size == 0)
				return null;
			Data newData = createNewData();
			doHardCopy(from, newData);
			return newData;
		}

		public void doHardCopy(Data from, Data to) {
			to.setNoData();
			copyHard(from, to);
		}

		public void doSoftCopy(Data from, Data to) {
			to.setNoData();
			moveContent(from, to);
		}

		public void doSet(Data from, Data to) {
			to.setNoData();
			moveContent(from, to);
			from.setNoData();
		}

		public Data doTake(Data target) {
			if (target == null || target.size == 0)
				return null;
			Data newData = createNewData();
			moveContent(target, newData);
			target.setNoData();
			return newData;
		}

		public void moveContent(Data from, Data to) {
			while (from.lsData.size() > 0) {
				ByteBuffer bb = from.lsData.remove(0);
				to.lsData.add(bb);
				to.size += bb.limit();
			}
		}

		public void copyHard(Data from, Data to) {
			int written = 0;
			for (ByteBuffer bb : from.lsData) {
				int limit = bb.limit();
				if (limit == 0)
					throw new RuntimeException("limit cannot be zero");

				ByteBuffer bbTo = (to.inv == null) ? obtainBuffer() : to.inv.takeEmptyBuffer();
				bbTo.put(bb.array(), 0, limit);
				to.add(bbTo);
				written += limit;
			}
			if (from.size != to.size)
				throw new RuntimeException("size doesn't match: src " + from.size + " dest: "
						+ to.size);
			to.postRead();
		}

	}

	public static final class Serializer {

		public BuffersOutputStream bbos = null;

		public BuffersInputStream bbis = null;

		public Serializer() {
			bbos = new BuffersOutputStream();
			bbis = new BuffersInputStream();
		}

		public static final int STREAM_END = 3234455;

		public void writeObject(BufferProvider bufferProvider, Object obj) throws Exception {
			bbos.setBufferProvider(bufferProvider);
			bbos.reset();
			if (obj instanceof byte[]) {
				bbos.write((byte) 40);
				byte[] bytes = (byte[]) obj;
				bbos.writeInt(bytes.length);
				bbos.write((byte[]) obj);
			} else if (obj instanceof Long) {
				bbos.write((byte) 41);
				bbos.writeLong(((Long) obj).longValue());
			} else if (obj instanceof Integer) {
				bbos.write((byte) 42);
				bbos.writeInt(((Integer) obj).intValue());
			} else if (obj instanceof DataSerializable) {
				bbos.write((byte) 45);
				bbos.writeUTF(obj.getClass().getName());
				((DataSerializable) obj).writeData(bbos);
				bbos.writeInt(STREAM_END);
			} else if (obj instanceof String) {
				bbos.write((byte) 46);
				String str = (String) obj;
				bbos.writeUTF(str); 
			} else {
				bbos.write((byte) 66);
				ObjectOutputStream os = new ObjectOutputStream(bbos);
				os.writeUnshared(obj);
			}
			bbos.flush();
		}

		public Object readObject(BufferProvider bufferProvider) {
			try {
				bbis.setBufferProvider(bufferProvider);
				bbis.reset();
				Object result = null;
				byte type = (byte) bbis.read();
				if (type == 40) {
					int size = bbis.readInt();
					byte[] bytes = new byte[size];
					bbis.read(bytes);
					result = bytes;
				} else if (type == 41) {
					result = Long.valueOf(bbis.readLong());
				} else if (type == 42) {
					result = Integer.valueOf(bbis.readInt());
				} else if (type == 45) { 
					String className = bbis.readUTF();
					DataSerializable ds = (DataSerializable) Class.forName(className).newInstance();
					ds.readData(bbis); 
					if (bbis.readInt() != STREAM_END)
						throw new RuntimeException("Unproper stream-end!");
					result = ds;
				} else if (type == 46) {
					result = bbis.readUTF(); 
				} else if (type == 66) {
					ObjectInputStream in = new ObjectInputStream(bbis);
					result = in.readUnshared();
				} else {
					throw new RuntimeException("Unknown readObject type " + type);
				}
				return result;
			} catch (Exception e) {
				e.printStackTrace();
				return null;
			}
		}
	}

	public final class Data implements DataSerializable {
		List<ByteBuffer> lsData = new ArrayList<ByteBuffer>(listMaxSize);

		int size = 0;

		ByteBuffer bbCurrentlyRead = null;

		int readSize = 0;

		Invocation inv = null;

		public Data() {
		}

		public Data(Invocation inv) {
			this();
			this.inv = inv;
		}

		public void digest(MessageDigest md) {
			for (ByteBuffer bb : lsData) {
				md.update(bb.array(), 0, bb.limit());
			}
		}

		public void copyToBuffer(ByteBuffer to) {
			int written = 0;
			for (ByteBuffer bb : lsData) {
				int limit = bb.limit();
				if (limit == 0)
					throw new RuntimeException("limit cannot be zero");
				to.put(bb.array(), 0, limit);
				written += limit;
			}
			if (written != size)
				throw new RuntimeException("copyToBuffer didn't write all data. Written: "
						+ written + " size: " + size);
		}

		public boolean shouldRead() {
			return (readSize < size);
		}

		public boolean add(ByteBuffer bb) {
			if (bb.position() == 0)
				throw new RuntimeException("Position cannot be 0");
			lsData.add(bb);
			size += bb.position();
			return true;
		}

		private void setNoData() {
			while (lsData.size() > 0) {
				ByteBuffer bb = lsData.remove(0);
				inv.addEmpty(bb);
			}
			prepareForRead();
		}

		public void prepareForRead() {
			if (lsData.size() > 0)
				throw new RuntimeException("prepareForRead has data " + this);
			size = 0;
			readSize = 0;
			bbCurrentlyRead = null;
			hash = Integer.MIN_VALUE;
		}

		public void read(ByteBuffer src) {
			while (true) {
				int remaining = size - readSize;
				if (bbCurrentlyRead == null) {
					bbCurrentlyRead = inv.takeEmptyBuffer();
					bbCurrentlyRead.limit((remaining > 1024) ? 1024 : remaining);
				}
				readSize += BufferUtil.copy(src, bbCurrentlyRead);
				if (readSize >= size) {
					if (bbCurrentlyRead.position() == 0)
						throw new RuntimeException("Position cannot be 0");
					lsData.add(bbCurrentlyRead);
					bbCurrentlyRead = null;
					return;
				}
				if (bbCurrentlyRead.remaining() == 0) {
					if (bbCurrentlyRead.position() == 0)
						throw new RuntimeException("Position cannot be 0");
					lsData.add(bbCurrentlyRead);
					bbCurrentlyRead = null;
				}
				if (src.remaining() == 0)
					return;
			}
		}

		public void postRead() {
			int totalRead = 0;
			int listSize = lsData.size();
			for (int i = 0; i < listSize; i++) {
				ByteBuffer bb = lsData.get(i);
				totalRead += bb.position();
				if (i < (listSize - 1) && bb.position() != 1024) {
					throw new RuntimeException(listSize + " This buffer size has to be 1024. "
							+ bb.position() + "  index: " + i);
				}
				if (i == (listSize - 1) && bb.position() == 0) {
					throw new RuntimeException("Last buffer cannot be zero. " + bb.position());
				}
				bb.flip();
			}

			if (totalRead != size)
				throw new RuntimeException(totalRead + " but size should be " + size);
			bbCurrentlyRead = null;
			readSize = 0;
		}

		@Override
		public String toString() {
			return "Data ls.size=" + lsData.size() + " size = " + size;
		}

		public boolean isEmpty() {
			return size == 0 && lsData.size() == 0;
		}

		public int size() {
			return size;
		}

		int hash = Integer.MIN_VALUE;

		@Override
		public int hashCode() {
			if (hash == Integer.MIN_VALUE) {
				int h = 1;
				for (ByteBuffer bb : lsData) {
					int limit = bb.limit();
					byte[] buffer = bb.array();
					for (int i = 0; i < limit; i++) {
						h = 31 * h + buffer[i];
					}
				}
				hash = h;
			}
			return hash;
		}

		@Override
		public boolean equals(Object obj) {
			if (this == obj)
				return true;
			Data data = (Data) obj;
			if (data.size != size)
				return false;
			int bufferCount = lsData.size();
			if (bufferCount != data.lsData.size())
				return false;
			for (int i = 0; i < bufferCount; i++) {
				ByteBuffer thisBB = lsData.get(i);
				byte[] thisBuffer = thisBB.array();
				byte[] dataBuffer = data.lsData.get(i).array();
				int limit = thisBB.limit();
				for (int b = 0; b < limit; b++) {
					if (thisBuffer[b] != dataBuffer[b])
						return false;
				}
			}
			return true;
		}

		public void readData(DataInput in) throws IOException {
			int size = in.readInt();
			int remaining = size;
			while (remaining > 0) {
				ByteBuffer bb = obtainBuffer();
				int sizeToRead = (remaining > 1024) ? 1024 : remaining;
				byte[] bytes = new byte[sizeToRead];
				in.readFully(bytes);
				bb.put(bytes);
				add(bb);
				remaining -= sizeToRead;
			}
			postRead();
		}

		public void writeData(DataOutput out) throws IOException {
			out.writeInt(size);
			for (ByteBuffer bb : lsData) {
				out.write(bb.array(), 0, bb.limit());
			}

		}

	}

}
