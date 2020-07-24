#include "ldb-jni_common.h"

extern "C" {

JNIEXPORT void JNICALL Java_net_daporkchop_ldbjni_natives_NativeWriteBatch_put0HH
  (JNIEnv* env, jobject obj, jlong ptr, jbyteArray key, jint keyOff, jint keyLen, jbyteArray value, jint valueOff, jint valueLen)  {
    auto writeBatch = (leveldb::WriteBatch*) ptr;

    if (writeBatch == nullptr)  {
        throwISE(env, "NativeWriteBatch has already been closed!");
        return;
    }

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

    leveldb::Slice keySlice(&keyPtr[keyOff], keyLen);
    leveldb::Slice valueSlice(&valuePtr[valueOff], valueLen);

    writeBatch->Put(keySlice, valueSlice);

    env->ReleasePrimitiveArrayCritical(value, valuePtr, 0);
    env->ReleasePrimitiveArrayCritical(key, keyPtr, 0);
}

JNIEXPORT void JNICALL Java_net_daporkchop_ldbjni_natives_NativeWriteBatch_put0HD
  (JNIEnv* env, jobject obj, jlong ptr, jbyteArray key, jint keyOff, jint keyLen, jlong valueAddr, jint valueOff, jint valueLen)  {
    auto writeBatch = (leveldb::WriteBatch*) ptr;

    if (writeBatch == nullptr)  {
        throwISE(env, "NativeWriteBatch has already been closed!");
        return;
    }

    auto keyPtr = (char*) env->GetPrimitiveArrayCritical(key, nullptr);
    if (!keyPtr)    {
        throwISE(env, "Unable to pin key array");
        return;
    }

    leveldb::Slice keySlice(&keyPtr[keyOff], keyLen);
    leveldb::Slice valueSlice((char*) valueAddr, valueLen);

    writeBatch->Put(keySlice, valueSlice);

    env->ReleasePrimitiveArrayCritical(key, keyPtr, 0);
}

JNIEXPORT void JNICALL Java_net_daporkchop_ldbjni_natives_NativeWriteBatch_put0DH
  (JNIEnv* env, jobject obj, jlong ptr, jlong keyAddr, jint keyLen, jbyteArray value, jint valueOff, jint valueLen)  {
    auto writeBatch = (leveldb::WriteBatch*) ptr;

    if (writeBatch == nullptr)  {
        throwISE(env, "NativeWriteBatch has already been closed!");
        return;
    }

    auto valuePtr = (char*) env->GetPrimitiveArrayCritical(value, nullptr);
    if (!valuePtr)    {
        throwISE(env, "Unable to pin value array");
        return;
    }

    leveldb::Slice keySlice((char*) keyAddr, keyLen);
    leveldb::Slice valueSlice(&valuePtr[valueOff], valueLen);

    writeBatch->Put(keySlice, valueSlice);

    env->ReleasePrimitiveArrayCritical(value, valuePtr, 0);
}

JNIEXPORT void JNICALL Java_net_daporkchop_ldbjni_natives_NativeWriteBatch_put0DD
  (JNIEnv* env, jobject obj, jlong ptr, jlong keyAddr, jint keyLen, jlong valueAddr, jint valueLen)  {
    auto writeBatch = (leveldb::WriteBatch*) ptr;

    if (writeBatch == nullptr)  {
        throwISE(env, "NativeWriteBatch has already been closed!");
        return;
    }

    leveldb::Slice keySlice((char*) keyAddr, keyLen);
    leveldb::Slice valueSlice((char*) valueAddr, valueLen);

    writeBatch->Put(keySlice, valueSlice);
}

JNIEXPORT void JNICALL Java_net_daporkchop_ldbjni_natives_NativeWriteBatch_delete0H
  (JNIEnv* env, jobject obj, jlong ptr, jbyteArray key, jint keyOff, jint keyLen)  {
    auto writeBatch = (leveldb::WriteBatch*) ptr;

    if (writeBatch == nullptr)  {
        throwISE(env, "NativeWriteBatch has already been closed!");
        return;
    }

    auto keyPtr = (char*) env->GetPrimitiveArrayCritical(key, nullptr);
    if (!keyPtr)    {
        throwISE(env, "Unable to pin key array");
        return;
    }

    leveldb::Slice keySlice(&keyPtr[keyOff], keyLen);

    writeBatch->Delete(keySlice);

    env->ReleasePrimitiveArrayCritical(key, keyPtr, 0);
}

JNIEXPORT void JNICALL Java_net_daporkchop_ldbjni_natives_NativeWriteBatch_delete0D
  (JNIEnv* env, jobject obj, jlong ptr, jlong keyAddr, jint keyLen)  {
    auto writeBatch = (leveldb::WriteBatch*) ptr;

    if (writeBatch == nullptr)  {
        throwISE(env, "NativeWriteBatch has already been closed!");
        return;
    }

    leveldb::Slice keySlice((char*) keyAddr, keyLen);

    writeBatch->Delete(keySlice);
}

}
