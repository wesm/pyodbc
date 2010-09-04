
#include "pyodbc.h"
#include "paramtypes.h"
#include "wrapper.h"

PyObject* ParamTypes_New(int count)
{
    ParamTypes* p = PyObject_NEW(ParamTypes, &ParamTypesType);
    if (!p)
        return 0;
    Object pT((PyObject*)p);

    p->count = count;
    p->types = (SQLSMALLINT*)malloc(count * sizeof(SQLSMALLINT));
    if (p->types == 0)
    {
        PyErr_NoMemory();
        return 0;
    }

    // SQL_UNKNOWN_TYPE is zero
    memset(p->types, 0, sizeof(SQLSMALLINT) * count);

    return pT.Detach();
}

PyTypeObject ParamTypesType =
{
    PyObject_HEAD_INIT(0)
    0,                                                      // ob_size
    "pyodbc.ParamTypes",                                    // tp_name
    sizeof(ParamTypes),                                     // tp_basicsize
    0,                                                      // tp_itemsize
    0,                                                      // destructor tp_dealloc
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
