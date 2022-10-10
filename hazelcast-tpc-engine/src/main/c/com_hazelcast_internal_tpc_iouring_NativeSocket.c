/*
 * Copyright (c) 2008-2022, Hazelcast, Inc. All Rights Reserved.
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
 */

#include <stdint.h>
#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <unistd.h>
#include <sys/eventfd.h>
#include <sys/socket.h>
#include <limits.h>
#include <time.h>
#include <arpa/inet.h>
#include <errno.h>
#include <netinet/tcp.h>
#include <fcntl.h>


#include "include/utils.h"
#include "include/com_hazelcast_internal_tpc_iouring_Linux.h"
#include "include/com_hazelcast_internal_tpc_iouring_NativeSocket.h"


static jclass class_SocketAddressFactory;
static jmethodID method_createIPv4Address;


static socklen_t sockaddr_from_java(JNIEnv* env, jobject address, jint port, struct sockaddr_storage* sa) {
    // todo: we always assume IPv4 here. When we add IPv6, more logic needs to be added
    jbyteArray array = (jbyteArray)address;
    struct sockaddr_in* sin = (struct sockaddr_in*)sa;
    sin->sin_family = AF_INET;
    sin->sin_port = htons(port);
    (*env)->GetByteArrayRegion(env, array, 0, 4, (jbyte*)&sin->sin_addr);
    return sizeof(struct sockaddr_in);
}

static jobject create_java_socket_address(JNIEnv* env, struct sockaddr_storage* addr, socklen_t addr_len) {
    if (addr->ss_family == AF_INET) {
        struct sockaddr_in* sin = (struct sockaddr_in*)addr;
        int ip = ntohl(sin->sin_addr.s_addr);
        int port = ntohs(sin->sin_port);
        return (*env)->CallStaticObjectMethod(env, class_SocketAddressFactory, method_createIPv4Address, ip, port);
    }

    throw_io_exception(env, "Unknown type of socket address", errno);
    return NULL;
}

JNIEXPORT jobject JNICALL
Java_com_hazelcast_internal_tpc_iouring_NativeSocket_toInetSocketAddress(JNIEnv* env, jclass this_class,
                                                                   jlong addr, jlong l){
    struct sockaddr_storage* sa = (struct sockaddr_storage*)addr;
    socklen_t sa_len = *((socklen_t*)l);

    return create_java_socket_address(env, (struct sockaddr_storage*)sa, sa_len);
}

JNIEXPORT void JNICALL
Java_com_hazelcast_internal_tpc_iouring_NativeSocket_initNative(JNIEnv* env, jclass this_class){
    class_SocketAddressFactory = (*env)->NewGlobalRef(env, (*env)->FindClass(
        env, "com/hazelcast/internal/tpc/iouring/SocketAddressFactory"));
    method_createIPv4Address = (*env)->GetStaticMethodID(
        env, class_SocketAddressFactory, "createIPv4Address", "(II)Ljava/net/InetSocketAddress;");
}

JNIEXPORT jint JNICALL
Java_com_hazelcast_internal_tpc_iouring_NativeSocket_socket(JNIEnv* env, jclass this_class, jint type,
                                                      jint domain, jint protocol){
    return socket(type, domain, protocol);
}

JNIEXPORT void JNICALL
Java_com_hazelcast_internal_tpc_iouring_NativeSocket_listen(JNIEnv* env, jclass this_class, jint sock_fd,
                                                      jint backlog){
    int res = listen(sock_fd, backlog);
    if (res == -1){
        throw_io_exception(env, "listen", errno);
        return;
    }
}

JNIEXPORT jint JNICALL
Java_com_hazelcast_internal_tpc_iouring_NativeSocket_bind(JNIEnv* env, jclass this_class, jint sock_fd,
                                                    jbyteArray local_addr, jint port){
    struct sockaddr_storage sa;
    socklen_t len = sockaddr_from_java(env, local_addr, port, &sa);

    int res = bind(sock_fd, (struct sockaddr*)&sa, len);
    if (res != 0) {
       throw_io_exception(env, "bind", errno);
    }
 }

JNIEXPORT void JNICALL
Java_com_hazelcast_internal_tpc_iouring_NativeSocket_connect(JNIEnv *env, jclass this_class, jint sock_fd,
                                                       jbyteArray remote_addr, jint port, jint family){
     struct sockaddr_storage sa;
     socklen_t len = sockaddr_from_java(env, remote_addr, port, &sa);

     while (connect(sock_fd, (struct sockaddr*)&sa, len) != 0) {
        if (errno != EINTR) {
            throw_io_exception(env, "connect", errno);
            break;
        }
     }
}

JNIEXPORT void JNICALL
Java_com_hazelcast_internal_tpc_iouring_NativeSocket_setTcpNoDelay(JNIEnv *env, jclass this_class, jint sock_fd,
                                                             jboolean tcpNoDelay){
    int option_value = tcpNoDelay;
    int option_len = sizeof(int);

    int res = setsockopt(sock_fd, IPPROTO_TCP, TCP_NODELAY, &option_value, option_len);
    if (res == -1) {
        throw_io_exception(env, "setTcpNoDelay", errno);
        return;
    }
}

JNIEXPORT jboolean JNICALL
Java_com_hazelcast_internal_tpc_iouring_NativeSocket_isTcpNoDelay(JNIEnv* env, jclass this_class, jint sock_fd){
    int option_value;
    int option_len = sizeof(option_value);

    int res = getsockopt(sock_fd, IPPROTO_TCP, TCP_NODELAY, &option_value, &option_len);
    if (res == -1) {
        return throw_io_exception(env, "isTcpNoDelay", errno);
    }
    return option_value;
}

JNIEXPORT void JNICALL
Java_com_hazelcast_internal_tpc_iouring_NativeSocket_setSendBufferSize(JNIEnv* env, jclass this_class, jint sock_fd,
                                                                 jint sendBufferSize){
    int option_value = sendBufferSize;
    int option_len = sizeof(option_value);

    int res = setsockopt(sock_fd, SOL_SOCKET, SO_SNDBUF, &option_value, option_len);
    if (res == -1) {
        throw_io_exception(env, "setSendBufferSize", errno);
        return;
    }
}

JNIEXPORT jint JNICALL
Java_com_hazelcast_internal_tpc_iouring_NativeSocket_getSendBufferSize(JNIEnv* env, jclass this_class, jint sock_fd){
    int option_value;
    int option_len = sizeof(option_value);

    int res = getsockopt(sock_fd, SOL_SOCKET, SO_SNDBUF, &option_value, &option_len);
    if (res == -1) {
        return throw_io_exception(env, "getSendBufferSize", errno);
    }
    return option_value;
}

JNIEXPORT void JNICALL
Java_com_hazelcast_internal_tpc_iouring_NativeSocket_setReceiveBufferSize(JNIEnv* env, jclass this_class, jint sock_fd,
                                                                    jint receiveBufferSize){
    int option_value = receiveBufferSize;
    int option_len = sizeof(option_value);

    int res = setsockopt(sock_fd, SOL_SOCKET, SO_RCVBUF, &option_value, option_len);
    if (res == -1) {
        throw_io_exception(env, "setReceiveBufferSize", errno);
        return;
    }
}

JNIEXPORT jint JNICALL
Java_com_hazelcast_internal_tpc_iouring_NativeSocket_getReceiveBufferSize(JNIEnv* env, jclass this_class, jint sock_fd){
    int option_value;
    int option_len = sizeof(option_value);

    int res = getsockopt(sock_fd, SOL_SOCKET, SO_RCVBUF, &option_value, &option_len);
    if (res == -1) {
        return throw_io_exception(env, "getReceiveBufferSize", errno);
    }
    return option_value;
}

JNIEXPORT jboolean JNICALL
Java_com_hazelcast_internal_tpc_iouring_NativeSocket_isReuseAddress(JNIEnv* env, jclass this_class, jint sock_fd){
    int option_value;
    int option_len = sizeof(option_value);

    int res = getsockopt(sock_fd, SOL_SOCKET, SO_REUSEADDR, &option_value, &option_len);
    if (res == -1) {
        return throw_io_exception(env, "isReuseAddress", errno);
    }
    return option_value;
}

JNIEXPORT void JNICALL
Java_com_hazelcast_internal_tpc_iouring_NativeSocket_setReuseAddress(JNIEnv* env, jclass this_class, jint sock_fd,
                                                               jboolean reuseAddress){
    int option_value = reuseAddress;
    int option_len = sizeof(option_value);

    int res = setsockopt(sock_fd, SOL_SOCKET, SO_REUSEADDR, &option_value, option_len);
    if (res == -1) {
         throw_io_exception(env, "setReuseAddress", errno);
         return;
    }
 }

JNIEXPORT jboolean JNICALL
Java_com_hazelcast_internal_tpc_iouring_NativeSocket_isReusePort(JNIEnv* env, jclass this_class, jint sock_fd){
    int option_value;
    int option_len = sizeof(option_value);

    int res = getsockopt(sock_fd, SOL_SOCKET, SO_REUSEPORT, &option_value, &option_len);
    if (res == -1) {
        return throw_io_exception(env, "isReusePort", errno);
    }
    return option_value;
}

JNIEXPORT void JNICALL
Java_com_hazelcast_internal_tpc_iouring_NativeSocket_setReusePort(JNIEnv* env, jclass this_class, jint sock_fd,
                                                            jboolean reusePort){
    int option_value = reusePort;
    int option_len = sizeof(option_value);

    int res = setsockopt(sock_fd, SOL_SOCKET, SO_REUSEPORT, &option_value, option_len);
    if (res == -1) {
        throw_io_exception(env, "setTcpNoDelay", errno);
        return;
    }
}

JNIEXPORT jboolean JNICALL
Java_com_hazelcast_internal_tpc_iouring_NativeSocket_isKeepAlive(JNIEnv* env, jclass this_class, jint sock_fd){
    int option_value;
    int option_len = sizeof(option_value);

    int res = getsockopt(sock_fd,  SOL_SOCKET, SO_KEEPALIVE, &option_value, &option_len);
    if (res == -1) {
        return throw_io_exception(env, "isKeepAlive", errno);
    }
    return option_value;
}

JNIEXPORT void JNICALL
Java_com_hazelcast_internal_tpc_iouring_NativeSocket_setKeepAlive(JNIEnv* env, jclass this_class, jint sock_fd,
                                                            jboolean keepAlive){
    int option_value = keepAlive;
    int option_len = sizeof(option_value);

    int res = setsockopt(sock_fd,  SOL_SOCKET, SO_KEEPALIVE, &option_value, option_len);
    if (res == -1) {
        throw_io_exception(env, "setKeepAlive", errno);
        return;
    }
}

JNIEXPORT jint JNICALL
Java_com_hazelcast_internal_tpc_iouring_NativeSocket_getTcpKeepAliveTime(JNIEnv* env, jclass this_class, jint sock_fd){
    int option_value;
    int option_len = sizeof(option_value);

    int res = getsockopt(sock_fd,  IPPROTO_TCP, TCP_KEEPIDLE, &option_value, &option_len);
    if (res == -1) {
        return throw_io_exception(env, "getTcpKeepAliveTime", errno);
    }
    return option_value;
}

JNIEXPORT void JNICALL
Java_com_hazelcast_internal_tpc_iouring_NativeSocket_setTcpKeepAliveTime(JNIEnv* env, jclass this_class, jint sock_fd,
                                                                   jint tcp_keepalive_time){
    int option_value = tcp_keepalive_time;
    int option_len = sizeof(option_value);

    int res = setsockopt(sock_fd,  IPPROTO_TCP, TCP_KEEPIDLE, &option_value, option_len);
    if (res == -1) {
        throw_io_exception(env, "setTcpKeepAliveTime", errno);
        return;
    }
}

JNIEXPORT jint JNICALL
Java_com_hazelcast_internal_tpc_iouring_NativeSocket_getTcpKeepaliveIntvl(JNIEnv* env, jclass this_class, jint sock_fd){
    int option_value;
    int option_len = sizeof(option_value);

    int res = getsockopt(sock_fd,  IPPROTO_TCP, TCP_KEEPINTVL, &option_value, &option_len);
    if (res == -1) {
        return throw_io_exception(env, "getTcpKeepaliveIntvl", errno);
    }
    return option_value;
}

JNIEXPORT void JNICALL
Java_com_hazelcast_internal_tpc_iouring_NativeSocket_setTcpKeepaliveIntvl(JNIEnv* env, jclass this_class, jint sock_fd,
                                                                    jint tcp_keepalive_intvl){
    int option_value = tcp_keepalive_intvl;
    int option_len = sizeof(option_value);

    int res = setsockopt(sock_fd,  IPPROTO_TCP, TCP_KEEPINTVL, &option_value, option_len);
    if (res == -1) {
        throw_io_exception(env, "setTcpKeepaliveIntvl", errno);
        return;
    }
}

JNIEXPORT jint JNICALL
Java_com_hazelcast_internal_tpc_iouring_NativeSocket_getTcpKeepaliveProbes(JNIEnv* env, jclass this_class, jint sock_fd){
    int option_value;
    int option_len = sizeof(option_value);

    int res = getsockopt(sock_fd,  IPPROTO_TCP, TCP_KEEPCNT, &option_value, &option_len);
    if (res == -1) {
        return throw_io_exception(env, "getTcpKeepaliveProbes", errno);
    }
    return option_value;
}

JNIEXPORT void JNICALL
Java_com_hazelcast_internal_tpc_iouring_NativeSocket_setTcpKeepAliveProbes(JNIEnv* env, jclass this_class, jint sock_fd,
                                                                     jint tcp_keepalive_probes){
    int option_value = tcp_keepalive_probes;
    int option_len = sizeof(option_value);

    int res = setsockopt(sock_fd,  IPPROTO_TCP, TCP_KEEPCNT, &option_value, option_len);
    if (res == -1) {
        throw_io_exception(env, "setTcpKeepAliveProbes", errno);
        return;
    }
}

JNIEXPORT void JNICALL
Java_com_hazelcast_internal_tpc_iouring_NativeSocket_setSoLinger(JNIEnv* env, jclass this_class, jint sock_fd,
                                                           jint soLinger){
    struct linger option_value;
    if (soLinger < 0){
        option_value.l_onoff = 0;
        option_value.l_linger = 0;
    } else {
        option_value.l_onoff = 1;
        option_value.l_linger = soLinger;
    }

    int option_len = sizeof(option_value);

    int res = setsockopt(sock_fd, SOL_SOCKET, SO_LINGER, &option_value, option_len);
    if (res == -1) {
        throw_io_exception(env, "setSoLinger", errno);
        return;
    }
}

JNIEXPORT jint JNICALL
Java_com_hazelcast_internal_tpc_iouring_NativeSocket_getSoLinger(JNIEnv* env, jclass this_class, jint sock_fd){
    struct linger option_value;
    int option_len = sizeof(option_value);

    int res = getsockopt(sock_fd, SOL_SOCKET, SO_LINGER, &option_value, &option_len);
    if (res == -1) {
        return throw_io_exception(env, "getSoLinger", errno);
    }

    return option_value.l_onoff == 0 ? -1 : option_value.l_linger;
}

JNIEXPORT jobject JNICALL
Java_com_hazelcast_internal_tpc_iouring_NativeSocket_getLocalAddress(JNIEnv* env, jclass this_class, jint sock_fd){
    struct sockaddr_storage addr;
    socklen_t addr_len = sizeof(addr);

    int res = getsockname(sock_fd, (struct sockaddr*)&addr, &addr_len);
    if (res != 0) {
        return NULL;
    }

    return create_java_socket_address(env, &addr, addr_len);
}

JNIEXPORT jobject JNICALL
Java_com_hazelcast_internal_tpc_iouring_NativeSocket_getRemoteAddress(JNIEnv* env, jclass this_class, jint sock_fd){
    struct sockaddr_storage addr;
    socklen_t addr_len = sizeof(addr);

    int res = getpeername(sock_fd, (struct sockaddr*)&addr, &addr_len);
    if (res != 0) {
        return NULL;
    }

    return create_java_socket_address(env, &addr, addr_len);
}

JNIEXPORT void JNICALL
Java_com_hazelcast_internal_tpc_iouring_NativeSocket_setBlocking(JNIEnv* env, jclass this_class,
                                                           jint socketfd, jboolean blocking){
    int res = fcntl(socketfd, F_GETFL, 0);
    if (res == -1){
        throw_io_exception(env, "Failed to call socket.setBlocking", errno);
        return;
    }

    int old_flags = res;
    int new_flags;
    if (blocking) {
        new_flags = old_flags & ~O_NONBLOCK;
    } else {
        new_flags = old_flags | O_NONBLOCK;
    }

    res = fcntl(socketfd, F_SETFL, new_flags);
    if (res == -1) {
        throw_io_exception(env, "Failed to call socket.setBlocking", errno);
        return;
    }
}

JNIEXPORT jboolean JNICALL
Java_com_hazelcast_internal_tpc_iouring_NativeSocket_isBlocking(JNIEnv* env, jclass this_class, jint socketfd){
    int res = fcntl(socketfd, F_GETFL, 0);
    if (res == -1){
        return throw_io_exception(env, "Failed to call socket.isBlocking", errno);
    }
    int flags = res;
    return (flags & O_NONBLOCK) == 0;
}
