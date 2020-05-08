#include <stdexcept>
extern "C" {
#include "postgres.h"
}

template <class T, MemoryContext *M = &CurrentMemoryContext>
class pg_allocator
{
public:
  typedef T value_type;
  typedef size_t size_type;
  typedef ptrdiff_t difference_type;

  typedef T* pointer;
  typedef const T* const_pointer;

  typedef T& reference;
  typedef const T& const_reference;

  pointer address(reference r) { return &r; }
  const_pointer address(const_reference r) { return &r; }

  pg_allocator() throw() { };
  template <class U> pg_allocator(const pg_allocator<U, M>&) throw() { };
  ~pg_allocator() throw() { };

  pointer allocate(size_type n, const T *hint = 0) throw(std::bad_alloc);
  void deallocate(pointer p, size_type n);

  void construct(pointer p, const T& val) { new(p) T(val); }
  void destroy(pointer p) { p->~T(); }

  size_type max_size() const throw();

  template <class U>
  struct rebind {typedef pg_allocator<U, M> other; };
};

template <class T, MemoryContext *M>
T* pg_allocator<T, M>::allocate(size_type n, const T * hint) throw(std::bad_alloc)
{
  void* ptr = 0;

  ptr = MemoryContextAlloc(*M, (n * sizeof(T)));

  if (ptr)
    return static_cast<T*>(ptr);

  throw std::bad_alloc();
}

template <class T, MemoryContext *M>
void pg_allocator<T, M>::deallocate(T* ptr, size_t n)
{
  pfree(static_cast<void *>(ptr));
}

template <class T, MemoryContext *M>
bool operator==(const pg_allocator<T, M>& a, const pg_allocator<T, M>& b) {
  return true;
}

template <class T, MemoryContext *M>
bool operator!=(const pg_allocator<T, M>& a, const pg_allocator<T, M>& b) {
  return false;
}