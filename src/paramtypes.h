
#ifndef PARAMCACHE_H
#define PARAMCACHE_H

extern PyTypeObject ParamTypesType;

struct ParamTypes
{
    PyObject_HEAD

    // The number of parameters.  This should never be zero since there is no reason to
    // store one of these structures if there are no parameters.
    int count;

    // An array of SQL types allocated via malloc.  These are initially set to SQL_UNKNOWN_TYPE and are only set when
    // binding NULL.
    SQLSMALLINT* types;
};

PyObject* ParamTypes_New(int count);

#endif // PARAMCACHE_H
