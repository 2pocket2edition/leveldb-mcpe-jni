#include "ldb-jni_common.h"

extern "C" {

JNIEXPORT void JNICALL Java_net_daporkchop_ldbjni_natives_NativeDBProvider_destroy0
  (JNIEnv* env, jclass cla, jstring name,
   jboolean create_if_missing, jboolean error_if_exists, jboolean paranoid_checks, jint write_buffer_size,
   jint max_open_files, jint block_size, jint block_restart_interval, jint max_file_size, jint compression, jlong cacheSize)  {
    leveldb::Options options;

    const char* name_native = env->GetStringUTFChars(name, nullptr);
    leveldb::Status status = leveldb::DestroyDB( name_native, options);
    env->ReleaseStringUTFChars(name, name_native);

    checkException(env, status);
}

JNIEXPORT void JNICALL Java_net_daporkchop_ldbjni_natives_NativeDBProvider_repair0
  (JNIEnv* env, jclass cla, jstring name,
   jboolean create_if_missing, jboolean error_if_exists, jboolean paranoid_checks, jint write_buffer_size,
   jint max_open_files, jint block_size, jint block_restart_interval, jint max_file_size, jint compression, jlong cacheSize)  {
    leveldb::Options options;

    loadOptions(env, options, create_if_missing, error_if_exists, paranoid_checks, write_buffer_size, max_open_files, block_size, block_restart_interval, max_file_size, compression, cacheSize);

    const char* name_native = env->GetStringUTFChars(name, nullptr);
    leveldb::Status status = leveldb::RepairDB( name_native, options);
    env->ReleaseStringUTFChars(name, name_native);

    checkException(env, status);
}

}
