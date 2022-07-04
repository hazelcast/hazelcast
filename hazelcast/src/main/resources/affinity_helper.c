#define _GNU_SOURCE
#include "affinity_helper.h"
#include <sched.h>
#include <unistd.h>
#include <sys/syscall.h>

void throw_runtime_exception(JNIEnv *env, const char *msg);

JNIEXPORT jobject JNICALL Java_com_hazelcast_internal_util_ThreadAffinityHelper_getAffinity0
  (JNIEnv *env, jobject obj)
{
    cpu_set_t cpumask;
    int cpu_count = (int) sysconf(_SC_NPROCESSORS_ONLN);
    int i;
    pid_t tid = syscall(SYS_gettid);

    jclass bitset_cls = (*env)->FindClass(env, "java/util/BitSet");
    if (bitset_cls == NULL) {
        throw_runtime_exception(env, "Couldn't find class java.util.BitSet");
        return NULL;
    }

    jmethodID bitset_constr = (*env)->GetMethodID(env, bitset_cls, "<init>", "()V");
    if (bitset_constr == NULL) {
        throw_runtime_exception(env, "Couldn't find default constructor of java.util.BitSet");
        return NULL;
    }

    jobject bitset_obj = (*env)->NewObject(env, bitset_cls, bitset_constr);
    if (bitset_constr == NULL) {
        throw_runtime_exception(env, "Couldn't create a java.util.BitSet with its default constructor");
        return NULL;
    }

    jmethodID bitset_set = (*env)->GetMethodID(env, bitset_cls, "set", "(I)V");
    if (bitset_set == NULL) {
        throw_runtime_exception(env, "Couldn't get the method id for java.util.BitSet#set(int)");
        return bitset_obj;
    }

    int rc = sched_getaffinity(tid, sizeof(cpu_set_t), &cpumask);
    if (rc != 0) {
        throw_runtime_exception(env, "Couldn't get the CPU affinity for this thread");
        return bitset_obj;
    }

    for (int i = 0; i < cpu_count; ++i) {
        if (CPU_ISSET(i, &cpumask)) {
            (*env)->CallVoidMethod(env, bitset_obj, bitset_set, i);
        }
    }

    return bitset_obj;
}

JNIEXPORT void JNICALL Java_com_hazelcast_internal_util_ThreadAffinityHelper_setAffinity0
  (JNIEnv *env, jobject obj, jobject bitset_obj)
{
    int cpu_count = (int) sysconf(_SC_NPROCESSORS_ONLN);
    int i;
    pid_t tid = syscall(SYS_gettid);

    jclass bitset_cls = (*env)->FindClass(env, "java/util/BitSet");
    if (bitset_cls == NULL) {
        throw_runtime_exception(env, "Couldn't find class java.util.BitSet");
        return;
    }

    jmethodID bitset_size = (*env)->GetMethodID(env, bitset_cls, "size", "()I");
    if (bitset_size == NULL) {
        throw_runtime_exception(env, "Couldn't get the method id for java.util.BitSet#size()");
        return;
    }

    jmethodID bitset_get = (*env)->GetMethodID(env, bitset_cls, "get", "(I)Z");
    if (bitset_get == NULL) {
        throw_runtime_exception(env, "Couldn't get the method id for java.util.BitSet#get(int)");
        return;
    }

    int size = (int) (*env)->CallIntMethod(env, bitset_obj, bitset_size);

    cpu_set_t cpumask;
    CPU_ZERO(&cpumask);
    for (int i = 0; i < cpu_count; ++i) {
        int bit_is_set = (int) (*env)->CallIntMethod(env, bitset_obj, bitset_get, i);
        if (bit_is_set == 1) {
            CPU_SET(i, &cpumask);
        }
    }

    int rc = sched_setaffinity(tid, sizeof(cpumask), &cpumask);
    if (rc != 0) {
        throw_runtime_exception(env, "Couldn't set the CPU affinity for this thread");
        return;
    }
}

void throw_runtime_exception(JNIEnv *env, const char *msg) {
    jclass exClass = (*env)->FindClass(env, "java/lang/RuntimeException");
    (*env)->ThrowNew(env, exClass, msg);
}
