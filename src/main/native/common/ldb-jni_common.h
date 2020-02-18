#ifndef _Included_ldb-jni_common_h
#define _Included_ldb-jni_common_h

#include <leveldb/db.h>

#include <jni.h>

bool checkException(JNIEnv* env, leveldb::Status& status);

#endif //_Included_ldb-jni_common_h
