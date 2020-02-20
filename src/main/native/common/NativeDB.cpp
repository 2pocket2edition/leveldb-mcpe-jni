#include "ldb-jni_common.h"
#include "NativeDB.h"

static jfieldID dbID;
static jfieldID dcaID;

JNIEXPORT void JNICALL Java_net_daporkchop_ldbjni_natives_NativeDB_init
  (JNIEnv* env, jclass cla)  {
    dbID  = env->GetFieldID(cla, "db", "J");
    dcaID = env->GetFieldID(cla, "dca", "J");
}

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

JNIEXPORT jbyteArray JNICALL Java_net_daporkchop_ldbjni_natives_NativeDB_get0
  (JNIEnv* env, jobject obj, jbyteArray key, jboolean verifyChecksums, jboolean fillCache, jlong snapshot)  {
    auto db = (leveldb::DB*) env->GetLongField(obj, dbID);

    leveldb::ReadOptions readOptions;
    readOptions.verify_checksums = verifyChecksums;
    readOptions.fill_cache = fillCache;
    readOptions.snapshot = (leveldb::Snapshot*) snapshot;
    readOptions.decompress_allocator = (leveldb::DecompressAllocator*) env->GetLongField(obj, dcaID);

    int keyLength = env->GetArrayLength(key);
    char* keyRaw = new char[keyLength];
    env->GetByteArrayRegion(key, 0, keyLength, (jbyte*) keyRaw);
    leveldb::Slice keySlice(keyRaw, keyLength);

    std::string value;
    leveldb::Status status = db->Get(readOptions, keySlice, &value);
    delete keyRaw;

    if (status.IsNotFound() || checkException(env, status))    {
        return (jbyteArray) nullptr;
    }

    jbyteArray out = env->NewByteArray(value.size());
    env->SetByteArrayRegion(out, 0, value.size(), (jbyte*) value.data());
    return out;
}

JNIEXPORT void JNICALL Java_net_daporkchop_ldbjni_natives_NativeDB_put0
  (JNIEnv* env, jobject obj, jbyteArray key, jbyteArray value, jboolean sync)  {
    auto db = (leveldb::DB*) env->GetLongField(obj, dbID);

    leveldb::WriteOptions writeOptions;
    writeOptions.sync = sync;

    int keyLength = env->GetArrayLength(key);
    char* keyRaw = new char[keyLength];
    env->GetByteArrayRegion(key, 0, keyLength, (jbyte*) keyRaw);
    leveldb::Slice keySlice(keyRaw, keyLength);

    int valueLength = env->GetArrayLength(value);
    char* valueRaw = new char[valueLength];
    env->GetByteArrayRegion(value, 0, valueLength, (jbyte*) valueRaw);
    leveldb::Slice valueSlice(valueRaw, valueLength);

    leveldb::Status status = db->Put(writeOptions, keySlice, valueSlice);
    delete keyRaw;
    delete valueRaw;

    checkException(env, status);
}

JNIEXPORT void JNICALL Java_net_daporkchop_ldbjni_natives_NativeDB_delete0
  (JNIEnv* env, jobject obj, jbyteArray key, jboolean sync)  {
    auto db = (leveldb::DB*) env->GetLongField(obj, dbID);

    leveldb::WriteOptions writeOptions;
    writeOptions.sync = sync;

    int keyLength = env->GetArrayLength(key);
    char* keyRaw = new char[keyLength];
    env->GetByteArrayRegion(key, 0, keyLength, (jbyte*) keyRaw);
    leveldb::Slice keySlice(keyRaw, keyLength);

    leveldb::Status status = db->Delete(writeOptions, keySlice);
    delete keyRaw;

    checkException(env, status);
}

JNIEXPORT jlong JNICALL Java_net_daporkchop_ldbjni_natives_NativeDB_createWriteBatch0
  (JNIEnv* env, jobject obj)  {
    return (jlong) new leveldb::WriteBatch();
}

JNIEXPORT void JNICALL Java_net_daporkchop_ldbjni_natives_NativeDB_releaseWriteBatch0
  (JNIEnv* env, jobject obj, jlong writeBatch)  {
    if ((leveldb::WriteBatch*) writeBatch == nullptr)  {
        throwISE(env, "NativeWriteBatch has already been closed!");
        return;
    }

    delete (leveldb::WriteBatch*) writeBatch;
}

JNIEXPORT void JNICALL Java_net_daporkchop_ldbjni_natives_NativeDB_writeBatch0
  (JNIEnv* env, jobject obj, jlong writeBatch, jboolean sync)  {
    if ((leveldb::WriteBatch*) writeBatch == nullptr)  {
        throwISE(env, "NativeWriteBatch has already been closed!");
        return;
    }

    auto db = (leveldb::DB*) env->GetLongField(obj, dbID);

    leveldb::WriteOptions writeOptions;
    writeOptions.sync = sync;

    leveldb::Status status = db->Write(writeOptions, (leveldb::WriteBatch*) writeBatch);
    checkException(env, status);
}

JNIEXPORT void JNICALL Java_net_daporkchop_ldbjni_natives_NativeDB_compactRange0
  (JNIEnv* env, jobject obj, jbyteArray start, jbyteArray limit)  {
    auto db = (leveldb::DB*) env->GetLongField(obj, dbID);

    char* startRaw;
    leveldb::Slice startSlice;
    char* limitRaw;
    leveldb::Slice limitSlice;

    if (start != nullptr)   {
        int startLength = env->GetArrayLength(start);
        startRaw = new char[startLength];
        env->GetByteArrayRegion(start, 0, startLength, (jbyte*) startRaw);
        startSlice = leveldb::Slice(startRaw, startLength);
    }
    if (limit != nullptr)   {
        int limitLength = env->GetArrayLength(limit);
        limitRaw = new char[limitLength];
        env->GetByteArrayRegion(limit, 0, limitLength, (jbyte*) limitRaw);
        limitSlice = leveldb::Slice(limitRaw, limitLength);
    }

    db->CompactRange(start == nullptr ? nullptr : &startSlice, limit == nullptr ? nullptr : &limitSlice);

    if (start != nullptr)   {
        delete startRaw;
    }
    if (limit != nullptr)   {
        delete limitRaw;
    }
}
