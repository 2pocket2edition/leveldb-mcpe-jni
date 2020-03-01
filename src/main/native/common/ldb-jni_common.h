#ifndef _Included_ldbjni_common_h
#define _Included_ldbjni_common_h

#include <leveldb/cache.h>
#include <leveldb/db.h>
#include <leveldb/decompress_allocator.h>
#include <leveldb/filter_policy.h>
#include <leveldb/write_batch.h>
#include <leveldb/zlib_compressor.h>

#include <jni.h>

bool checkException(JNIEnv* env, leveldb::Status& status);

jint throwNPE(JNIEnv* env, const char* msg);

jint throwISE(JNIEnv* env, const char* msg);

jint throwException(JNIEnv* env, const char* msg);

jint throwException(JNIEnv* env, const char* msg, jint err);

jint throwException(JNIEnv* env, const char* msg, jlong err);

void loadOptions(JNIEnv* env, leveldb::Options& options,
     jboolean create_if_missing, jboolean error_if_exists, jboolean paranoid_checks, jint write_buffer_size,
     jint max_open_files, jint block_size, jint block_restart_interval, jint max_file_size, jint compression, jlong cacheSize);

#endif //_Included_ldbjni_common_h
