
#include "pyodbc.h"
#include "errors.h"
#include "lrucache.h"
#include "wrapper.h"

PyObject* LRUCache_New(int size)
{
    LRUCache* p = PyObject_NEW(LRUCache, &LRUCacheType);
    if (!p)
        return 0;
    Object holder((PyObject*)p);

    p->size = size;
    p->dict = 0;
    p->list = 0;

    p->dict = PyDict_New();
    p->list = PyList_New(0);

    if (!p->dict || !p->list)
        return 0;

    return holder.Detach();
}

static void LRUCache_Del(PyObject* p)
{
    LRUCache* cache = (LRUCache*)p;

    Py_XDECREF(cache->dict);
    Py_XDECREF(cache->list);
    
    PyObject_Del(p);
}

bool LRUCache::add(PyObject* key, PyObject* value)
{
    I(PyList_GET_SIZE(list) == PyDict_Size(dict));
    TRACE("LRU: add\n");

    // If the item is already in the cache, move it to the beginning.

    if (PyDict_Contains(dict, key))
        return move_to_front(key);

    if (PyList_GET_SIZE(list) == size)
    {
        // Remove the last item.
        TRACE("LRU: Removing last item\n");
        PyObject* oldkey = PyList_GET_ITEM(list, PyList_GET_SIZE(list)-1);
        PyDict_DelItem(dict, oldkey);
        PySequence_DelItem(list, PyList_GET_SIZE(list)-1);
    }

    if (PyDict_SetItem(dict, key, value) != 0)
        return false;

    if (PyList_Insert(list, 0, key) != 0)
    {
        // Now we have a problem because we've removed an item from the list but can't reinsert it.  Remove it from
        // the dictionary so at least they stay in sync.
        PyDict_DelItem(dict, key);
        return false;
    }

    TRACE("LRU: Added\n");

    I(PyList_GET_SIZE(list) == PyDict_Size(dict));

    return true;
}

bool LRUCache::move_to_front(PyObject* key)
{
    PyObject* first = PyList_GET_ITEM(list, 0);
    if (first == key)
    {
        TRACE("LRU: already at front\n");
        return true; // already in first place
    }
    
    int index = PySequence_Index(list, key);

    // I(index > 0);
    if (index == -1)
    {
        RaiseErrorV("HY000", 0, "move_to_front failed");
        return false;
    }
    
    PySequence_DelItem(list, index);

    TRACE("LRU: moving from %d to front\n", index);

    if (PyList_Insert(list, 0, key) != 0)
    {
        // Now we have a problem because we've removed an item from the list but can't reinsert it.  Remove it from
        // the dictionary so at least they stay in sync.
        PyDict_DelItem(dict, key);
        return false;
    }

    return true;
}

PyObject* LRUCache::get(PyObject* key)
{
    I(PyList_GET_SIZE(list) == PyDict_Size(dict));

    PyObject* value = PyDict_GetItem(dict, key);
    if (value == 0)
    {
        TRACE("LRU: cache miss\n");
        return 0;
    }
    
    if (!move_to_front(key))
        return 0;
                                
    TRACE("LRU: cache hit\n");

    Py_INCREF(value);
    return value;
}


PyTypeObject LRUCacheType =
{
    PyObject_HEAD_INIT(0)
    0,                                                      // ob_size
    "pyodbc.LRUCache",                                      // tp_name
    sizeof(LRUCache),                                       // tp_basicsize
    0,                                                      // tp_itemsize
    LRUCache_Del,                                           // destructor tp_dealloc
    0,                                                      // tp_print
    0,                                                      // tp_getattr
    0,                                                      // tp_setattr
    0,                                                      // tp_compare
    0,                                                      // tp_repr
    0,                                                      // tp_as_number
    0,                                                      // tp_as_sequence
    0,                                                      // tp_as_mapping
    0,                                                      // tp_hash
    0,                                                      // tp_call
    0,                                                      // tp_str
    0,                                                      // tp_getattro
    0,                                                      // tp_setattro
    0,                                                      // tp_as_buffer
    Py_TPFLAGS_DEFAULT,                                     // tp_flags
};
