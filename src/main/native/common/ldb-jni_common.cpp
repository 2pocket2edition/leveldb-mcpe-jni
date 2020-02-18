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
