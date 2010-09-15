
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

PyObject* PyUnicode_FromSQLWCHAR(const SQLWCHAR* sz, Py_ssize_t cch);

bool sqlwchar_copy(SQLWCHAR* pdest, const Py_UNICODE* psrc, Py_ssize_t len);

SQLWCHAR* SQLWCHAR_FromUnicode(const Py_UNICODE* pch, Py_ssize_t len);

#if defined(MS_WIN32) && Py_UNICODE_SIZE == 2
STATIC_ASSERT(sizeof(SQLWCHAR) == sizeof(Py_UNICODE), SQLAndUnicodeDiffer);
STATIC_ASSERT(sizeof(SQLWCHAR) == sizeof(wchar_t), SQLAndWcharDiffer);
#define UnicodeSizesDiffer() false
#define UNICODE_SAME_SIZE 1
#else
inline bool UnicodeSizesDiffer() 
{
    return sizeof(SQLWCHAR) != sizeof(Py_UNICODE);
}
#endif

#endif // _PYODBCSQLWCHAR_H
