#include "ldb-jni_common.h"
#include "NativeWriteBatch.h"

JNIEXPORT void JNICALL Java_net_daporkchop_ldbjni_natives_NativeWriteBatch_put0
  (JNIEnv* env, jobject obj, jlong ptr, jbyteArray key, jbyteArray value)  {
    auto writeBatch = (leveldb::WriteBatch*) ptr;

    if (writeBatch == nullptr)  {
        throwISE(env, "NativeWriteBatch has already been closed!");
        return;
    }

    int keyLength = env->GetArrayLength(key);
    int valueLength = env->GetArrayLength(value);

    auto keyPtr = (char*) env->GetPrimitiveArrayCritical(key, nullptr);
    if (!keyPtr)    {
        throwISE(env, "Unable to pin key array");
        return;
    }

    auto valuePtr = (char*) env->GetPrimitiveArrayCritical(value, nullptr);
    if (!valuePtr)    {
        env->ReleasePrimitiveArrayCritical(key, keyPtr, 0);
        throwISE(env, "Unable to pin value array");
        return;
    }

    leveldb::Slice keySlice(keyPtr, keyLength);
    leveldb::Slice valueSlice(valuePtr, valueLength);

    writeBatch->Put(keySlice, valueSlice);

    env->ReleasePrimitiveArrayCritical(value, valuePtr, 0);
    env->ReleasePrimitiveArrayCritical(key, keyPtr, 0);
}

JNIEXPORT void JNICALL Java_net_daporkchop_ldbjni_natives_NativeWriteBatch_delete0
  (JNIEnv* env, jobject obj, jlong ptr, jbyteArray key)  {
    auto writeBatch = (leveldb::WriteBatch*) ptr;

    if (writeBatch == nullptr)  {
        throwISE(env, "NativeWriteBatch has already been closed!");
        return;
    }

    int keyLength = env->GetArrayLength(key);

    auto keyPtr = (char*) env->GetPrimitiveArrayCritical(key, nullptr);
    if (!keyPtr)    {
        throwISE(env, "Unable to pin key array");
        return;
    }

    leveldb::Slice keySlice(keyPtr, keyLength);

    env->ReleasePrimitiveArrayCritical(key, keyPtr, 0);
}
