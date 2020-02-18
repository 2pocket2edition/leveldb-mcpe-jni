#include "ldb-jni_common.h"
#include "NativeDB.h"

JNIEXPORT jlong JNICALL Java_net_daporkchop_ldbjni_natives_NativeDB_openDb
  (JNIEnv* env, jclass cla, jstring name, jboolean create_if_missing, jboolean error_if_exists, jboolean paranoid_checks, jint write_buffer_size,
   jint max_open_files, jint block_size, jint block_restart_interval, jint max_file_size, jint compression, jlong cacheSize)  {
    leveldb::DB* db;
    leveldb::Options options;

    options.create_if_missing = create_if_missing;
    options.error_if_exists = error_if_exists;
    options.paranoid_checks = paranoid_checks;
    options.write_buffer_size = (size_t) write_buffer_size;
    options.max_open_files = max_open_files;
    options.block_size = (size_t) block_size;
    options.block_restart_interval = block_restart_interval;
    //options.max_file_size = (size_t) max_file_size;

    switch (compression)    {
        case 0x01: //SNAPPY
            //enabled by default
            break;
        case 0x04: //ZLIB_RAW
        	options.compressors[0] = new leveldb::ZlibCompressorRaw(-1);
        	options.compressors[1] = new leveldb::ZlibCompressor(); //for compatibility
        	break;
        default:
            throwException(env, "Invalid compression type (must be SNAPPY or ZLIB_RAW):", compression);
            return (jlong) nullptr;
    }

    options.filter_policy = leveldb::NewBloomFilterPolicy(10);
    options.block_cache = leveldb::NewLRUCache(cacheSize <= 0 ? 40 * 1024 * 1024 : cacheSize);

    const char* name_native = env->GetStringUTFChars(name, nullptr);
    leveldb::Status status = leveldb::DB::Open(options, name_native, &db);
    env->ReleaseStringUTFChars(name, name_native);

    if (checkException(env, status)) {
        return (jlong) nullptr;
    }

    return (jlong) db;
}

JNIEXPORT jlong JNICALL Java_net_daporkchop_ldbjni_natives_NativeDB_createDecompressAllocator
  (JNIEnv* env, jclass cla)  {
    return (jlong) new leveldb::DecompressAllocator();
}

JNIEXPORT void JNICALL Java_net_daporkchop_ldbjni_natives_NativeDB_closeDb
  (JNIEnv* env, jclass cla, jlong db, jlong dca)  {
    delete (leveldb::DB*) db;
    delete (leveldb::DecompressAllocator*) dca;
}
