
#ifndef _WRAPPER_H_
#define _WRAPPER_H_

#include "sqlwchar.h"

struct Object
{
    PyObject* p;

    // GCC freaks out if these are private, but it doesn't use them (?)
    // Object(const Object& illegal);
    // void operator=(const Object& illegal);

    Object(PyObject* _p = 0)
    {
        p = _p;
    }

    ~Object()
    {
        Py_XDECREF(p);
    }

    Object& operator=(PyObject* pNew)
    {
        Py_XDECREF(p);
        p = pNew;
        return *this;
    }

    bool IsValid() const { return p != 0; }

    void Attach(PyObject* _p)
    {
        Detach();
        p = _p;
    }

    PyObject* Detach()
    {
        PyObject* pT = p;
        p = 0;
        return pT;
    }

    operator PyObject*() 
    {
        return p;
    }

    PyObject* Get()
    {
        return p;
    }
};


struct Unicode : public Object
{
    bool Append(PyObject* other)
    {
        if (!other)
            return false;

        if (p == 0)
        {
            Attach(other);
            Py_INCREF(other);
            return true;
        }

        PyUnicode_Append(&p, other);
        return p != 0;
    }

    bool Append(const char* s)
    {
        Object other(PyUnicode_FromString(s));
        return Append(other);
    }

    bool Append(const SQLWCHAR* s)
    {
        Object other(PyUnicode_FromSQLWCHAR(s));
        return Append(other);
    }

    bool Append(long l)
    {
        Object longObj(PyLong_FromLong(l));
        Object unicodeObj = _PyLong_Format(longObj, 10);
        return Append(unicodeObj);
    }

    Py_ssize_t Size()
    {
        if (!p)
            return 0;
        return PyUnicode_GetSize(p);
    }
};



#ifdef WINVER
struct RegKey
{
    HKEY hkey;

    RegKey()
    {
        hkey = 0;
    }

    ~RegKey()
    {
        if (hkey != 0)
            RegCloseKey(hkey);
    }

    operator HKEY() { return hkey; }
};
#endif

#endif // _WRAPPER_H_

