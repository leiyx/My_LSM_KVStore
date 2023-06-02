#ifndef STORAGE_XDB_UTIL_THREAD_ANNOTATIONS_H_
#define STORAGE_XDB_UTIL_THREAD_ANNOTATIONS_H_

#if !defined(THREAD_ANNOTATION_ATTRIBUTE__)

#if defined(__clang__)
    #define THREAD_ANNOTATION_ATTRIBUTE__(x) __attribute__((x))
#else
    #define THREAD_ANNOTATION_ATTRIBUTE__(x)
#endif

#endif // !define(THREAD_ANNOTATION_ATTRIBUTE__)

#ifndef GUARDED_BY
#define GUARDED_BY(x) THREAD_ANNOTATION_ATTRIBUTE__(guarded_by(x))
#endif

#ifndef PT_GUARDED_BY
#define PT_GUARDED_BY(x) THREAD_ANNOTATION_ATTRIBUTE__(pt_guarded_by(x))
#endif

#ifndef EXCLUSIVE_LOCK_FUNCTION
#define EXCLUSIVE_LOCK_FUNCTION(...) \
    THREAD_ANNOTATION_ATTRIBUTE__(exclusive_lock_function(__VA_ARGS__))
#endif

#ifndef SHARED_LOCK_FUNCTION
#define SHARED_LOCK_FUNCTION(...) \
    THREAD_ANNOTATION_ATTRIBUTE__(shared_lock_function(__VA_ARGS__))
#endif

#ifndef UNLOCK_FUNCTION
#define UNLOCK_FUNCTION(...) \
    THREAD_ANNOTATION_ATTRIBUTE__(unlock_function(__VA_ARGS__))
#endif

#ifndef EXCLUSIVE_LOCKS_REQUIRED
#define EXCLUSIVE_LOCKS_REQUIRED(...) \
    THREAD_ANNOTATION_ATTRIBUTE__(exclusive_locks_required(__VA_ARGS__))
#endif

#ifndef ASSERT_EXCLUSIVE_LOCK
#define ASSERT_EXCLUSIVE_LOCK(...) \
  THREAD_ANNOTATION_ATTRIBUTE__(assert_exclusive_lock(__VA_ARGS__))
#endif

#ifndef ASSERT_SHARED_LOCK
#define ASSERT_SHARED_LOCK(...) \
  THREAD_ANNOTATION_ATTRIBUTE__(assert_shared_lock(__VA_ARGS__))
#endif

#ifndef LOCKABLE
#define LOCKABLE THREAD_ANNOTATION_ATTRIBUTE__(lockable)
#endif 

#ifndef SCOPED_LOCKABLE
#define SCOPED_LOCKABLE THREAD_ANNOTATION_ATTRIBUTE__(scoped_lockable)
#endif

#endif // STORAGE_XDB_UTIL_THREAD_ANNOTATIONS_H_