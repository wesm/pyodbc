
#ifndef LRUCACHE_H
#define LRUCACHE_H

extern PyTypeObject LRUCacheType;

struct LRUCache
{
    PyObject_HEAD

    int size;
    PyObject* dict;
    PyObject* list;

    PyObject* get(PyObject* key);
    bool add(PyObject* key, PyObject* value);

    bool move_to_front(PyObject* key);
};

PyObject* LRUCache_New(int size);

#endif //  LRUCACHE_H
