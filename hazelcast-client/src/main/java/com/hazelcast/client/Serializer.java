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

package com.hazelcast.client;


import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;

import com.hazelcast.nio.DataSerializable;

public class Serializer {
    private static final byte SERIALIZER_TYPE_DATA = 0;

    private static final byte SERIALIZER_TYPE_OBJECT = 1;

    private static final byte SERIALIZER_TYPE_BYTE_ARRAY = 2;

    private static final byte SERIALIZER_TYPE_INTEGER = 3;

    private static final byte SERIALIZER_TYPE_LONG = 4;

    private static final byte SERIALIZER_TYPE_CLASS = 5;

    private static final byte SERIALIZER_TYPE_STRING = 6;
    
    private static final int STRING_CHUNK_SIZE = 16*1024;

	public static byte[] toByte(Object object){
		ByteArrayOutputStream bos = new ByteArrayOutputStream();
		DataOutputStream dos = new DataOutputStream(bos);
		if(object==null) return new byte[0];
		try {
			if(object instanceof DataSerializable){
				dos.writeByte(SERIALIZER_TYPE_DATA);
				dos.writeUTF(object.getClass().getName().replaceFirst("com.hazelcast.client", "com.hazelcast"));
				((DataSerializable) object).writeData(dos);			
			}
			else if(object instanceof String){
				String string = (String)object;
				dos.writeByte(SERIALIZER_TYPE_STRING);
				int length = string.length();
				int chunkSize = length/STRING_CHUNK_SIZE+1;
				for(int i=0;i<chunkSize;i++){
					int beginIndex = Math.max(0,i*STRING_CHUNK_SIZE-1);
					int endIndex = Math.min((i+1)*STRING_CHUNK_SIZE-1, length);
					dos.writeUTF(string.substring(beginIndex, endIndex));
				}
			}
			else if(object instanceof byte[]){
				byte[] bytes = (byte[]) object;
				dos.writeByte(SERIALIZER_TYPE_BYTE_ARRAY);
				bos.write(bytes.length);
				bos.write(bytes);
			}
			else if(object instanceof Integer){
				dos.writeByte(SERIALIZER_TYPE_INTEGER);
				dos.writeInt((Integer)object);
			}
			else if(object instanceof Long){
				dos.writeByte(SERIALIZER_TYPE_LONG);
				dos.writeLong((Long)object);
			}
			else if(object instanceof Class){
				dos.writeByte(SERIALIZER_TYPE_CLASS);
				dos.writeUTF(((Class<?>)object).getName());
			}
			else{
				dos.writeByte(SERIALIZER_TYPE_OBJECT);
				ObjectOutputStream os = new ObjectOutputStream(dos);
				os.writeObject(object);
			}
		}catch(IOException e){
			e.printStackTrace();
		}
		return bos.toByteArray();
		
	}
	
	public static Object toObject(byte[] bytes){ 
		ByteArrayInputStream bis = new ByteArrayInputStream(bytes);
		DataInputStream dis = new DataInputStream(bis);
		int type = bis.read();
		try {
			if(type == SERIALIZER_TYPE_DATA){
				String className = dis.readUTF();

                if(className.equals("com.hazelcast.impl.Keys")){
                    className = "com.hazelcast.client.impl.Keys";
                }
                else if(className.equals("com.hazelcast.impl.CMap$Values")){
                    className = "com.hazelcast.client.impl.Values";
                }
				DataSerializable data = (DataSerializable)Class.forName(className).newInstance();

				data.readData(dis);
				return data;
			}
			else if(type == SERIALIZER_TYPE_STRING){
				StringBuilder result = new StringBuilder();
					while(dis.available()>0){
						result.append(dis.readUTF());
					}
				return result.toString();
			}
			else if(type == SERIALIZER_TYPE_BYTE_ARRAY){
				int size = dis.readInt();
				byte[] b = new byte[size];
				bis.read(b);
				return b;
			}
			else if(type == SERIALIZER_TYPE_INTEGER){
				return dis.readInt();
			}
			else if(type == SERIALIZER_TYPE_LONG){
				return dis.readLong();
			}
			else if(type == SERIALIZER_TYPE_CLASS){
				return Class.forName(dis.readUTF());
			}
			else if(type == SERIALIZER_TYPE_OBJECT){
				ObjectInputStream os = new ObjectInputStream(dis);
				return os.readObject();
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
		return null;
	}
//	public static final byte[] writeUTF(final String str) throws IOException {
//        final int strlen = str.length();
//        int utflen = 0;
//        int c, count = 0;
//
//        /* use charAt instead of copying String to char array */
//        for (int i = 0; i < strlen; i++) {
//            c = str.charAt(i);
//            if ((c >= 0x0001) && (c <= 0x007F)) {
//                utflen++;
//            } else if (c > 0x07FF) {
//                utflen += 3;
//            } else {
//                utflen += 2;
//            }
//        }
//
//        final byte[] bytearr = new byte[utflen + 4];
//
//        bytearr[count++] = (byte) ((utflen >>> 24) & 0xFF);
//        bytearr[count++] = (byte) ((utflen >>> 16) & 0xFF);
//        bytearr[count++] = (byte) ((utflen >>> 8) & 0xFF);
//        bytearr[count++] = (byte) ((utflen) & 0xFF);
//
//        int i;
//        for (i = 0; i < strlen; i++) {
//            c = str.charAt(i);
//            if (!((c >= 0x0001) && (c <= 0x007F)))
//                break;
//            bytearr[count++] = (byte) c;
//        }
//
//        for (; i < strlen; i++) {
//            c = str.charAt(i);
//            if ((c >= 0x0001) && (c <= 0x007F)) {
//                bytearr[count++] = (byte) c;
//
//            } else if (c > 0x07FF) {
//                bytearr[count++] = (byte) (0xE0 | ((c >> 12) & 0x0F));
//                bytearr[count++] = (byte) (0x80 | ((c >> 6) & 0x3F));
//                bytearr[count++] = (byte) (0x80 | ((c) & 0x3F));
//            } else {
//                bytearr[count++] = (byte) (0xC0 | ((c >> 6) & 0x1F));
//                bytearr[count++] = (byte) (0x80 | ((c) & 0x3F));
//            }
//        }
//        return bytearr;
//    }
//	class UTFReader{
//		private byte[] bytes;
//		private int pos;
//		private int remaining;
//
//		UTFReader(byte[] bytes){
//			this.bytes = bytes;
//		}
//		
//	
//	    public final String readUTF(byte[] bytes) throws IOException {
//	        final int utflen = readInt(bytes);
//	        byte[] bytearr = null;
//	        char[] chararr = null;
//	
//	        bytearr = new byte[utflen];
//	        chararr = new char[utflen];
//	
//	        int c, char2, char3;
//	        int count = 0;
//	        int chararr_count = 0;
//	
//	        readFully(bytearr, 0, utflen);
//	
//	        while (count < utflen) {
//	            c = bytearr[count] & 0xff;
//	            if (c > 127)
//	                break;
//	            count++;                   
//	            chararr[chararr_count++] = (char) c;
//	        }
//	
//	        while (count < utflen) {
//	            c = bytearr[count] & 0xff;
//	            switch (c >> 4) {
//	                case 0:
//	                case 1:
//	                case 2:
//	                case 3:
//	                case 4:
//	                case 5:
//	                case 6:
//	                case 7:
//	                    /* 0xxxxxxx */
//	                    count++;
//	                    chararr[chararr_count++] = (char) c;
//	                    break;
//	                case 12:
//	                case 13:
//	                    /* 110x xxxx 10xx xxxx */
//	                    count += 2;
//	                    if (count > utflen)
//	                        throw new UTFDataFormatException("malformed input: partial character at end");
//	                    char2 = bytearr[count - 1];
//	                    if ((char2 & 0xC0) != 0x80)
//	                        throw new UTFDataFormatException("malformed input around byte " + count);
//	                    chararr[chararr_count++] = (char) (((c & 0x1F) << 6) | (char2 & 0x3F));
//	                    break;
//	                case 14:
//	                    /* 1110 xxxx 10xx xxxx 10xx xxxx */
//	                    count += 3;
//	                    if (count > utflen)
//	                        throw new UTFDataFormatException("malformed input: partial character at end");
//	                    char2 = bytearr[count - 2];
//	                    char3 = bytearr[count - 1];
//	                    if (((char2 & 0xC0) != 0x80) || ((char3 & 0xC0) != 0x80))
//	                        throw new UTFDataFormatException("malformed input around byte " + (count - 1));
//	                    chararr[chararr_count++] = (char) (((c & 0x0F) << 12) | ((char2 & 0x3F) << 6) | ((char3 & 0x3F) << 0));
//	                    break;
//	                default:
//	                    /* 10xx xxxx, 1111 xxxx */
//	                    throw new UTFDataFormatException("malformed input around byte " + count);
//	            }
//	        }
//	        // The number of chars produced may be less than utflen
//	        return new String(chararr, 0, chararr_count);
//	    }
//	    
//	    public final int readInt(byte[] bytes) throws IOException {
//	        final int ch1 = read(bytes);
//	        final int ch2 = read(bytes);
//	        final int ch3 = read(bytes);
//	        final int ch4 = read(bytes);
//	        if ((ch1 | ch2 | ch3 | ch4) < 0)
//	            throw new EOFException();
//	        return ((ch1 << 24) + (ch2 << 16) + (ch3 << 8) + (ch4 << 0));
//	    }
//	    public int read(byte[] bytes) {
//	        if (!check(bytes))
//	            return -1;
//	        final int x = bytes[pos] & 0xff;
//	        move(1);
//	        return x;
//	    }
//	    private boolean check(byte[] bytes) {
//	        if (bytes == null || remaining <= 0) {
//	            return next();
//	        }
//	        return true;
//	    }
//	    private void move(final int x) {
//	        pos += x;
//	        remaining -= x;
//	        if (remaining < 0)
//	            throw new RuntimeException();
//	        if (pos > BYTE_BUFFER_SIZE)
//	            throw new RuntimeException();
//	    }
//	    private boolean next() {
//	        if (remaining != 0)
//	            throw new RuntimeException("Remaining should be zero " + remaining);
//	        remaining = bytes.length;
//	        pos = 0;
//	        return true;
//	    }
//	    
//	}
}
