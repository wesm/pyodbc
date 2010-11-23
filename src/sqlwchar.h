
#ifndef _PYODBCSQLWCHAR_H
#define _PYODBCSQLWCHAR_H

class SQLWChar
{
    // An object designed to convert strings and Unicode objects to SQLWCHAR, hold the temporary buffer, and delete it
    // in the destructor.

private:
    SQLWCHAR* pch;
    Py_ssize_t len;
    bool owns_memory;

public:
    SQLWChar()
    {
        pch = 0;
        len = 0;
        owns_memory = false;
    }

    SQLWChar(PyObject* o);

    bool Convert(PyObject* o);

    void Free();

    ~SQLWChar()
    {
        Free();
    }

    void dump();

    operator SQLWCHAR*() { return pch; }
    operator const SQLWCHAR*() const { return pch; }
    operator bool() const { return pch != 0; }
    Py_ssize_t size() const { return len; }

    SQLWCHAR* operator[] (Py_ssize_t i)
    {
        I(i <= len); // we'll allow access to the NULL?
        return &pch[i];
    }
        
    const SQLWCHAR* operator[] (Py_ssize_t i) const
    {
        I(i <= len); // we'll allow access to the NULL?
        return &pch[i];
    }
};

// Allocate a new Unicode object, initialized from the given SQLWCHAR string.
PyObject* PyUnicode_FromSQLWCHAR(const SQLWCHAR* sz, Py_ssize_t cch);

inline Py_ssize_t sqlwcslen(const SQLWCHAR* s)
{
#if SQLWCHAR_SIZE == Py_UNICODE_SIZE
    return Py_UNICODE_strlen((const Py_UNICODE*)s);
#elif defined(HAVE_WCHAR) && SQLWCHAR_SIZE == WCHAR_T_SIZE
    return wcslen((const wchar_t*)s);
#else
    Py_ssize_t len = 0;
    while (*s++ != 0)
        len++;
    return len;
#endif
}

inline PyObject* PyUnicode_FromSQLWCHAR(const SQLWCHAR* s)
{
    return PyUnicode_FromSQLWCHAR(s, sqlwcslen(s));
}

#if SQLWCHAR_SIZE != Py_UNCODE_SIZE
SQLWCHAR* SQLWCHAR_FromUnicode(const Py_UNICODE* pch, Py_ssize_t len);
#endif

bool SQLWCHAR_Same(const SQLWCHAR* lhs, const Py_UNICODE* rhs);

// This copies the low bytes, and should only be used when the text is *known* to be ANSI.
void copy_sqlstate(char* dest, const SQLWCHAR* src);

#endif // _PYODBCSQLWCHAR_H

