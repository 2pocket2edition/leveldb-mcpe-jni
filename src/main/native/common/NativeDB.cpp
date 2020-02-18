#include "ldb-jni_common.h"
#include "NativeDB.h"

JNIEXPORT jlong JNICALL Java_net_daporkchop_ldbjni_natives_NativeDB_openDb
  (JNIEnv* env, jclass cla, jstring name)  {
    leveldb::DB* db;
    leveldb::Options options;
    options.create_if_missing = true;

    const char* name_native = env->GetStringUTFChars(name, nullptr);
    leveldb::Status status = leveldb::DB::Open(options, name_native, &db);
    env->ReleaseStringUTFChars(name, name_native);

    if (checkException(env, status)) {
        return (jlong) nullptr;
    }

    return (jlong) db;
}

JNIEXPORT void JNICALL Java_net_daporkchop_ldbjni_natives_NativeDB_closeDb
  (JNIEnv* env, jclass cla, jlong dbAddr)  {
    delete (leveldb::DB*) dbAddr;
}
