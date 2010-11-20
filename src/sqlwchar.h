
#ifndef _PYODBCSQLWCHAR_H
#define _PYODBCSQLWCHAR_H

class SQLWChar
{
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

#if defined(MS_WIN32) && Py_UNICODE_SIZE == 2
inline PyObject* PyUnicode_FromSQLWCHAR(const SQLWCHAR* s) { return PyUnicode_FromWideChar((const wchar_t*)s, wcslen(s)); }
inline PyObject* PyUnicode_FromSQLWCHAR(const SQLWCHAR* s, size_t l) { return PyUnicode_FromWideChar((const wchar_t*)s, l); }
#else
PyObject* PyUnicode_FromSQLWCHAR(const SQLWCHAR* sz);
PyObject* PyUnicode_FromSQLWCHAR(const SQLWCHAR* sz, Py_ssize_t cch);
#endif

bool sqlwchar_copy(SQLWCHAR* pdest, const Py_UNICODE* psrc, Py_ssize_t len);

SQLWCHAR* SQLWCHAR_FromUnicode(const Py_UNICODE* pch, Py_ssize_t len);

inline bool UnicodeSizesDiffer() 
{
    return sizeof(SQLWCHAR) != sizeof(Py_UNICODE);
}

bool SQLWCHAR_Same(const SQLWCHAR* lhs, const Py_UNICODE* rhs);

// This copies the low bytes, and should only be used when the text is *known* to be ANSI.
void copy_sqlstate(char* dest, const SQLWCHAR* src);


#endif // _PYODBCSQLWCHAR_H
