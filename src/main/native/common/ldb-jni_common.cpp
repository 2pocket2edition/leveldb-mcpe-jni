#include "ldb-jni_common.h"

bool checkException(JNIEnv* env, leveldb::Status& status)   {
    if (!status.ok())   {
        const char* name = "java/lang/RuntimeException";
        if (status.IsCorruption())   {
            name = "net/daporkchop/ldbjni/exception/DBCorruptionException";
        } else if (status.IsNotFound()) {
            name = "net/daporkchop/ldbjni/exception/DBNotFoundException";
        } else if (status.IsIOError()) {
            name = "java/io/IOException";
        } else if (status.IsNotSupportedError())    {
            name = "java/lang/UnsupportedOperationException";
        } else if (status.IsInvalidArgument())  {
            name = "java/lang/IllegalArgumentException";
        }
        std::string msg = status.ToString();

        jclass clazz = env->FindClass(name);
        env->Throw((jthrowable) env->NewObject(
            clazz,
            env->GetMethodID(clazz, "<init>", "(Ljava/lang/String;)V"),
            env->NewStringUTF(msg.c_str())
        ));
        return true;
    } else {
        return false;
    }
}

jint throwNPE(JNIEnv* env, const char* msg)  {
    jclass clazz = env->FindClass("java/lang/NullPointerException");

    return env->Throw((jthrowable) env->NewObject(
        clazz,
        env->GetMethodID(clazz, "<init>", "(Ljava/lang/String;)V"),
        env->NewStringUTF(msg)
    ));
}

jint throwISE(JNIEnv* env, const char* msg)  {
    jclass clazz = env->FindClass("java/lang/IllegalStateException");

    return env->Throw((jthrowable) env->NewObject(
        clazz,
        env->GetMethodID(clazz, "<init>", "(Ljava/lang/String;)V"),
        env->NewStringUTF(msg)
    ));
}

jint throwException(JNIEnv* env, const char* msg)  {
    jclass clazz = env->FindClass("net/daporkchop/lib/natives/NativeException");

    return env->Throw((jthrowable) env->NewObject(
        clazz,
        env->GetMethodID(clazz, "<init>", "(Ljava/lang/String;)V"),
        env->NewStringUTF(msg)
    ));
}

jint throwException(JNIEnv* env, const char* msg, jint err)  {
    jclass clazz = env->FindClass("net/daporkchop/lib/natives/NativeException");

    return env->Throw((jthrowable) env->NewObject(
        clazz,
        env->GetMethodID(clazz, "<init>", "(Ljava/lang/String;I)V"),
        env->NewStringUTF(msg),
        err
    ));
}

jint throwException(JNIEnv* env, const char* msg, jlong err)  {
    jclass clazz = env->FindClass("net/daporkchop/lib/natives/NativeException");

    return env->Throw((jthrowable) env->NewObject(
        clazz,
        env->GetMethodID(clazz, "<init>", "(Ljava/lang/String;J)V"),
        env->NewStringUTF(msg),
        err
    ));
}

void loadOptions(JNIEnv* env, leveldb::Options& options,
     jboolean create_if_missing, jboolean error_if_exists, jboolean paranoid_checks, jint write_buffer_size,
     jint max_open_files, jint block_size, jint block_restart_interval, jint max_file_size, jint compression, jlong cacheSize)   {
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
            return;
    }

    options.filter_policy = leveldb::NewBloomFilterPolicy(10);
    options.block_cache = leveldb::NewLRUCache(cacheSize <= 0 ? 40 * 1024 * 1024 : cacheSize);
}
