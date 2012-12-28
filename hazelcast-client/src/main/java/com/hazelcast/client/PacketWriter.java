//<<<<<<< HEAD
/////*
//// * Copyright (c) 2008-2012, Hazel Bilisim Ltd. All Rights Reserved.
//// *
//// * Licensed under the Apache License, Version 2.0 (the "License");
//// * you may not use this file except in compliance with the License.
//// * You may obtain a copy of the License at
//// *
//// * http://www.apache.org/licenses/LICENSE-2.0
//// *
//// * Unless required by applicable law or agreed to in writing, software
//// * distributed under the License is distributed on an "AS IS" BASIS,
//// * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//// * See the License for the specific language governing permissions and
//// * limitations under the License.
//// */
////
////package com.hazelcast.client;
////
////import java.io.DataOutputStream;
////import java.io.IOException;
////import java.nio.ByteBuffer;
////import java.util.HashMap;
////import java.util.Map;
////
////public class PacketWriter extends PacketHandler {
////
////    final ByteBuffer writeHeaderBuffer = ByteBuffer.allocate(1 << 10); // 1k
////
////    final Map<String, byte[]> nameCache = new HashMap<String, byte[]>(100);
////
////    public void write(Connection connection, Packet request) throws IOException {
////        if (connection != null) {
////            final DataOutputStream dos = connection.getOutputStream();
////            if (!connection.headersWritten) {
////                dos.write(HEADER);
////                dos.flush();
////                connection.headersWritten = true;
////            }
////            request.writeTo(this, dos);
////        }
////    }
////
////    public void flush(Connection connection) throws IOException {
////        if (connection != null) {
////            DataOutputStream dos = connection.getOutputStream();
////            dos.flush();
////        }
////    }
////}
//=======
///*
// * Copyright (c) 2008-2012, Hazelcast, Inc. All Rights Reserved.
// *
// * Licensed under the Apache License, Version 2.0 (the "License");
// * you may not use this file except in compliance with the License.
// * You may obtain a copy of the License at
// *
// * http://www.apache.org/licenses/LICENSE-2.0
// *
// * Unless required by applicable law or agreed to in writing, software
// * distributed under the License is distributed on an "AS IS" BASIS,
// * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// * See the License for the specific language governing permissions and
// * limitations under the License.
// */
//
//package com.hazelcast.client;
//
//import java.io.DataOutputStream;
//import java.io.IOException;
//import java.nio.ByteBuffer;
//import java.util.HashMap;
//import java.util.Map;
//
//public class PacketWriter extends PacketHandler {
//
//    final ByteBuffer writeHeaderBuffer = ByteBuffer.allocate(1 << 10); // 1k
//
//    final Map<String, byte[]> nameCache = new HashMap<String, byte[]>(100);
//
//    public void write(Connection connection, Packet request) throws IOException {
//        if (connection != null) {
//            final DataOutputStream dos = connection.getOutputStream();
//            if (!connection.headersWritten) {
//                dos.write(HEADER);
//                dos.flush();
//                connection.headersWritten = true;
//            }
//            request.writeTo(this, dos);
//        }
//    }
//
//    public void flush(Connection connection) throws IOException {
//        if (connection != null) {
//            DataOutputStream dos = connection.getOutputStream();
//            dos.flush();
//        }
//    }
//}
//>>>>>>> bb991443394929adafae1c05186f60a71d229896
