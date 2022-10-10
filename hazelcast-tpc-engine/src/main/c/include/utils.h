#include <jni.h>
#include <stdint.h>

#ifndef utils_h
#define utils_h

int32_t throw_exception(JNIEnv *env, char *cause, int32_t ret);

int32_t throw_io_exception(JNIEnv *env, char *cause, int32_t ret);

int32_t throw_out_of_memory_error(JNIEnv *env);

#endif
