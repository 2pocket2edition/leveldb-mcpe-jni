#ifndef _Included_ldbjni_common_h
#define _Included_ldbjni_common_h

#include <leveldb/db.h>

#include <jni.h>

bool checkException(JNIEnv* env, leveldb::Status& status);

#endif //_Included_ldbjni_common_h
