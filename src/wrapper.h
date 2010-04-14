
#ifndef _WRAPPER_H_
#define _WRAPPER_H_

class Object
{
protected:
    PyObject* p;

    // GCC freaks out if these are private, but it doesn't use them (?)
    // Object(const Object& illegal);
    // void operator=(const Object& illegal);

public:
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
        // Does not incref!
        Attach(pNew);
        return *this;
    }

    bool IsValid() const { return p != 0; }

    void AttachAndRef(PyObject* _p)
    {
        Py_XDECREF(p);
        p = _p;
        Py_XINCREF(p);
    }
    
    void Attach(PyObject* _p)
    {
        Py_XDECREF(p);
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

class List : public Object
{
public:
    List(PyObject* p) 
        : Object(p)
    {
    }

    Py_ssize_t len() const { return PyList_GET_SIZE(p); }
    int append(PyObject* obj) { return PyList_Append(p, obj); }

    PyObject* GET(int i) { return PyList_GET_ITEM(p, i); }
    PyObject* SET(int i, PyObject* obj) { return PyList_SET_ITEM(p, i, obj); }
};


#endif // _WRAPPER_H_
