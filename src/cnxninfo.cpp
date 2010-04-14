
// There is a bunch of information we want from connections which requires calls to SQLGetInfo when we first connect.
// However, this isn't something we really want to do for every connection, so we cache it by the hash of the
// connection string.  When we create a new connection, we copy the values into the connection structure.
//
// We hash the connection string since it may contain sensitive information we wouldn't want exposed in a core dump.

#include "pyodbc.h"
#include "cnxninfo.h"
#include "connection.h"
#include "wrapper.h"

// Maps from a Python string of the SHA1 hash to a CnxnInfo object.
//
static PyObject* map_hash_to_info;

static PyObject* hashlib;       // The hashlib module
static PyObject* update;        // The string 'update', used in GetHash.

void CnxnInfo_init()
{
    // Called during startup to give us a chance to import the hash code.

    map_hash_to_info = PyDict_New();
    update = PyUnicode_FromString("update");
    hashlib = PyImport_ImportModule("hashlib");
}

static PyObject* GetHash(PyObject* p)
{
    if (hashlib)
    {
        // REVIEW: Have to encode Unicode objects to bytes first.  This is starting to sound expensive.
        // Is there a better way to handle this?

        // Object hash(PyObject_CallMethod(hashlib, "new", "s", "sha1"));
        // if (!hash.IsValid())
        //     return 0;
        //  
        // PyObject_CallMethodObjArgs(hash, update, p, 0);
        //  
        // return PyObject_CallMethod(hash, "hexdigest", 0);

        Py_INCREF(p);
        return p;
    }

    return 0;
}


static PyObject* CnxnInfo_New(Connection* cnxn)
{
    CnxnInfo* p = PyObject_NEW(CnxnInfo, &CnxnInfoType);
    if (!p)
        return 0;
    Object info((PyObject*)p);

    // set defaults
    p->odbc_major             = 3;
    p->odbc_minor             = 50;
    p->supports_describeparam = false;
    p->datetime_precision     = 19; // default: "yyyy-mm-dd hh:mm:ss"

    // WARNING: The GIL lock is released for the *entire* function here.  Do not touch any objects, call Python APIs,
    // etc.  We are simply making ODBC calls and setting atomic values (ints & chars).  Also, make sure the lock gets
    // released -- do not add an early exit.

    SQLRETURN ret;
    Py_BEGIN_ALLOW_THREADS

    char szVer[20];
    SQLSMALLINT cch = 0;
    ret = SQLGetInfo(cnxn->hdbc, SQL_DRIVER_ODBC_VER, szVer, _countof(szVer), &cch);
    if (SQL_SUCCEEDED(ret))
    {
        char* dot = strchr(szVer, '.');
        if (dot)
        {
            *dot = '\0';
            p->odbc_major=(char)atoi(szVer);
            p->odbc_minor=(char)atoi(dot + 1);
        }
    }

    char szYN[2];
    ret = SQLGetInfo(cnxn->hdbc, SQL_DESCRIBE_PARAMETER, szYN, _countof(szYN), &cch);
    if (SQL_SUCCEEDED(ret))
    {
        p->supports_describeparam = szYN[0] == 'Y';
    }

    // What is the datetime precision?  This unfortunately requires a cursor (HSTMT).

    HSTMT hstmt = 0;
    if (SQL_SUCCEEDED(SQLAllocHandle(SQL_HANDLE_STMT, cnxn->hdbc, &hstmt)))
    {
        if (SQL_SUCCEEDED(SQLGetTypeInfo(hstmt, SQL_TYPE_TIMESTAMP)) && SQL_SUCCEEDED(SQLFetch(hstmt)))
        {
            SQLINTEGER columnsize;
            if (SQL_SUCCEEDED(SQLGetData(hstmt, 3, SQL_INTEGER, &columnsize, sizeof(columnsize), 0)))
            {
                p->datetime_precision = columnsize;
            }
        }

        SQLFreeStmt(hstmt, SQL_CLOSE);
    }

    Py_END_ALLOW_THREADS

    // WARNING: Released the lock now.
    
    return info.Detach();
}


PyObject* GetConnectionInfo(PyObject* pConnectionString, Connection* cnxn)
{
    // Looks-up or creates a CnxnInfo object for the given connection string.  The connection string can be a Unicode
    // or String object.

    Object hash(GetHash(pConnectionString));

    if (hash.IsValid())
    {
        PyObject* info = PyDict_GetItem(map_hash_to_info, hash);

        if (info)
        {
            Py_INCREF(info);
            return info;
        }
    }

    PyObject* info = CnxnInfo_New(cnxn);
    if (info != 0 && hash.IsValid())
        PyDict_SetItem(map_hash_to_info, hash, info);

    return info;
}


PyTypeObject CnxnInfoType =
{
    PyObject_HEAD_INIT(0)
    "pyodbc.CnxnInfo",          // tp_name
    sizeof(CnxnInfo),           // tp_basicsize
    0,                          // tp_itemsize
    0,                          // tp_dealloc
    0,                          // tp_print
    0,                          // tp_getattr
    0,                          // tp_setattr
    0,                          // tp_reserved
    0,                          // tp_repr
    0,                          // tp_as_number
    0,                          // tp_as_sequence
    0,                          // tp_as_mapping
    0,                          // tp_hash 
    0,                          // tp_call
    0,                          // tp_str
    0,                          // tp_getattro
    0,                          // tp_setattro
    0,                          // tp_as_buffer
    Py_TPFLAGS_DEFAULT,         // tp_flags
    0,                          // tp_doc
};
