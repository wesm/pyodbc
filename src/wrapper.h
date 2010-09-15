
#ifndef _WRAPPER_H_
#define _WRAPPER_H_

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

struct Tuple : public Object
{
    Tuple(Py_ssize_t size)
    {
        p = PyTuple_New(size);
    }

    Py_ssize_t Size() { return PyTuple_GET_SIZE(p); }

    PyObject* GetItem(Py_ssize_t i) { return PyTuple_GET_ITEM(p, i); }
    void SetItem(Py_ssize_t i, PyObject* obj) { PyTuple_SET_ITEM(p, i, obj); }
};
    

#endif // _WRAPPER_H_
