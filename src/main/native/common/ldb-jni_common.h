#ifndef _Included_ldbjni_common_h
#define _Included_ldbjni_common_h

#include <leveldb/cache.h>
#include <leveldb/db.h>
#include <leveldb/decompress_allocator.h>
#include <leveldb/filter_policy.h>
#include <leveldb/zlib_compressor.h>

#include <jni.h>

bool checkException(JNIEnv* env, leveldb::Status& status);

jint throwNPE(JNIEnv* env, const char* msg);

jint throwException(JNIEnv* env, const char* msg);

jint throwException(JNIEnv* env, const char* msg, jint err);

jint throwException(JNIEnv* env, const char* msg, jlong err);

#endif //_Included_ldbjni_common_h
