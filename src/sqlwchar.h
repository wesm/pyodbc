
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

    operator SQLWCHAR*() { return pch; }
    operator const SQLWCHAR*() const { return pch; }
    operator bool() const { return pch != 0; }
    Py_ssize_t size() const { return len; }
};

PyObject* PyUnicode_FromSQLWCHAR(const SQLWCHAR* sz, Py_ssize_t cch);

bool sqlwchar_copy(SQLWCHAR* pdest, const Py_UNICODE* psrc, Py_ssize_t len);


#endif // _PYODBCSQLWCHAR_H
