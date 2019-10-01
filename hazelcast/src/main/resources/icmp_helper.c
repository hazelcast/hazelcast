#include <unistd.h>
#include <netinet/in.h>
#include "icmp_helper.h"

JNIEXPORT jboolean JNICALL Java_com_hazelcast_internal_util_ICMPHelper_isRawSocketPermitted0
        (JNIEnv *env, jobject obj)
{
    jint fd = socket(AF_INET, SOCK_RAW, IPPROTO_ICMP);
    if (fd != -1)
    {
        close(fd);
        return JNI_TRUE;
    }

    return JNI_FALSE;
}