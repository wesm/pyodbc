
#include "pyodbc.h"
#include "errors.h"
#include "pyodbcmodule.h"
#include "sqlwchar.h"
#include "wrapper.h"

// Exceptions

struct SqlStateMapping
{
    SQLWCHAR* prefix;
    int   prefix_len;
    PyObject** pexc_class;      // Note: Double indirection (pexc_class) necessary because the pointer values are not
                                // initialized during startup
};

static const struct SqlStateMapping sql_state_mapping[] =
{
    { L"0A000", 5, &NotSupportedError },
    { L"40002", 5, &IntegrityError },
    { L"22",    2, &DataError },
    { L"23",    2, &IntegrityError },
    { L"24",    2, &ProgrammingError },
    { L"25",    2, &ProgrammingError },
    { L"42",    2, &ProgrammingError },
    { L"HYT00", 5, &OperationalError },
    { L"HYT01", 5, &OperationalError },
};


static PyObject* ExceptionFromSqlState(const SQLWCHAR* sqlstate)
{
    // Returns the appropriate Python exception class given a SQLSTATE value.

    if (sqlstate && *sqlstate)
    {
        for (size_t i = 0; i < _countof(sql_state_mapping); i++)
            if (memcmp(sqlstate, sql_state_mapping[i].prefix, sql_state_mapping[i].prefix_len) == 0)
                return *sql_state_mapping[i].pexc_class;
    }

    return Error;
}

PyObject* RaiseError(PyObject* exc_class, const char* format, ...)
{
    va_list marker;
    va_start(marker, format);
    PyObject* pMsg = PyUnicode_FromFormatV(format, marker);
    va_end(marker);
    if (!pMsg)
    {
        PyErr_NoMemory();
        return 0;
    }

    PyErr_SetObject(exc_class, pMsg);
    Py_DECREF(pMsg);
    return 0;
}


PyObject* RaiseErrorV(const SQLWCHAR* sqlstate, PyObject* exc_class, const char* format, ...)
{
    PyObject *pAttrs = 0, *pError = 0;

    if (!sqlstate || !*sqlstate)
        sqlstate = L"HY000";

    if (!exc_class)
        exc_class = ExceptionFromSqlState(sqlstate);

    // Note: Don't use any native strprintf routines.  With Py_ssize_t, we need "%zd", but VC .NET doesn't support it.
    // PyString_FromFormatV already takes this into account.

    va_list marker;
    va_start(marker, format);
    PyObject* pMsg = PyUnicode_FromFormatV(format, marker);
    va_end(marker);
    if (!pMsg)
    {
        PyErr_NoMemory();
        return 0;
    }

    // Create an exception with a 'sqlstate' attribute (set to None if we don't have one) whose 'args' attribute is a
    // tuple containing the message and sqlstate value.  The 'sqlstate' attribute ensures it is easy to access in
    // Python (and more understandable to the reader than ex.args[1]), but putting it in the args ensures it shows up
    // in logs because of the default repr/str.

    pAttrs = Py_BuildValue("(Os)", pMsg, sqlstate);
    if (pAttrs)
    {
        pError = PyEval_CallObject(exc_class, pAttrs);
        if (pError)
            RaiseErrorFromException(pError);
    }
    
    Py_DECREF(pMsg);
    Py_XDECREF(pAttrs);
    Py_XDECREF(pError);

    return 0;
}



bool HasSqlState(PyObject* ex, const SQLWCHAR* szSqlState)
{
    // Returns true if `ex` is an exception and has the given SQLSTATE.  It is safe to pass 0 for ex.

    bool has = false;

    if (ex)
    {
        PyObject* args = PyObject_GetAttrString(ex, "args");
        if (args != 0)
        {
            PyObject* s = PySequence_GetItem(args, 1);
            if (s != 0 && PyUnicode_Check(s))
            {
                const Py_UNICODE* sz = PyUnicode_AsUnicode(s);
                if (sz && SQLWCHAR_Same(szSqlState, sz))
                    has = true;
            }
            Py_XDECREF(s);
            Py_DECREF(args);
        }
    }

    return has;
}


static PyObject* GetError(const SQLWCHAR* sqlstate, PyObject* exc_class, PyObject* pMsg)
{
    // pMsg
    //   The error message.  This function takes ownership of this object, so we'll free it if we fail to create an
    //   error.

    PyObject *pSqlState=0, *pAttrs=0, *pError=0;

    if (!sqlstate || !*sqlstate)
        sqlstate = L"HY000";

    if (!exc_class)
        exc_class = ExceptionFromSqlState(sqlstate);

    pAttrs = PyTuple_New(2);
    if (!pAttrs)
    {
        Py_DECREF(pMsg);
        return 0;
    }
    
    PyTuple_SetItem(pAttrs, 1, pMsg); // pAttrs now owns the pMsg reference; steals a reference, does not increment

    pSqlState = PyUnicode_FromSQLWCHAR(sqlstate);
    if (!pSqlState)
    {
        Py_DECREF(pAttrs);
        return 0;
    }
    
    PyTuple_SetItem(pAttrs, 0, pSqlState); // pAttrs now owns the pSqlState reference

    pError = PyEval_CallObject(exc_class, pAttrs); // pError will incref pAttrs

    Py_XDECREF(pAttrs);

    return pError;
}

static const char* DEFAULT_ERROR = "The driver did not supply an error!";

PyObject* RaiseErrorFromHandle(const char* szFunction, HDBC hdbc, HSTMT hstmt)
{
    // The exception is "set" in the interpreter.  This function returns 0 so this can be used in a return statement.

    PyObject* pError = GetErrorFromHandle(szFunction, hdbc, hstmt);

    if (pError)
    {
        RaiseErrorFromException(pError);
        Py_DECREF(pError);
    }
        
    return 0;
}

PyObject* GetErrorFromHandle(const char* szFunction, HDBC hdbc, HSTMT hstmt)
{
    TRACE("In RaiseError(%s)!\n", szFunction);

    // Creates and returns an exception from ODBC error information.
    // 
    // ODBC can generate a chain of errors which we concatenate into one error message.  We use the SQLSTATE from the
    // first message, which seems to be the most detailed, to determine the class of exception.
    //
    // If the function fails, for example, if it runs out of memory, zero is returned.
    //
    // szFunction
    //   The name of the function that failed.  Python generates a useful stack trace, but we often don't know where in
    //   the C++ code we failed.

    SQLSMALLINT nHandleType;
    SQLHANDLE   h;

    SQLWCHAR sqlstate[6] = L"";
    SQLINTEGER nNativeError;
    SQLSMALLINT cchMsg;

    SQLWCHAR sqlstateT[6];
    SQLWCHAR szMsg[1024];

    if (hstmt != SQL_NULL_HANDLE)
    {
        nHandleType = SQL_HANDLE_STMT;
        h = hstmt;
    }
    else if (hdbc != SQL_NULL_HANDLE)
    {
        nHandleType = SQL_HANDLE_DBC;
        h = hdbc;
    }
    else
    {
        nHandleType = SQL_HANDLE_ENV;
        h = henv;
    }

    Unicode msg;

    // unixODBC + PostgreSQL driver 07.01.0003 (Fedora 8 binaries from RPMs) crash if you call SQLGetDiagRec more
    // than once.  I hate to do this, but I'm going to only call it once for non-Windows platforms for now...

    SQLSMALLINT iRecord = 1;

    for (;;)
    {
        szMsg[0]     = 0;
        sqlstateT[0] = 0;
        nNativeError = 0;
        cchMsg       = 0;

        SQLRETURN ret;
        Py_BEGIN_ALLOW_THREADS
        ret = SQLGetDiagRecW(nHandleType, h, iRecord, (SQLWCHAR*)sqlstateT, &nNativeError, (SQLWCHAR*)szMsg, (short)(_countof(szMsg)-1), &cchMsg);
        Py_END_ALLOW_THREADS
        if (!SQL_SUCCEEDED(ret))
            break;

        // Not always NULL terminated (MS Access)
        sqlstateT[5] = 0;

        if (cchMsg != 0)
        {
            if (iRecord == 1)
            {
                // This is the first error message, so save the SQLSTATE for determining the exception class and append
                // the calling function name.
                memcpy(sqlstate, sqlstateT, sizeof(sqlstate[0]) * _countof(sqlstate));
            }
            else
            {
                // This is not the first message, so add a separator.
                if (!msg.Append("; "))
                    return 0;
            }

            if (!msg.Append("[") || !msg.Append(sqlstateT) || !msg.Append("] ") || !msg.Append(szMsg) ||
                !msg.Append(" (") || !msg.Append((long)nNativeError) || !msg.Append(") (") || !msg.Append(szFunction) || !msg.Append(")"))
            {
                return 0;
            }
        }

        iRecord++;

#ifndef _MSC_VER
        // See non-Windows comment above
        break;
#endif
    }

    if (msg.Size() == 0)
    {
        // This only happens using unixODBC.  (Haven't tried iODBC yet.)  Either the driver or the driver manager is
        // buggy and has signaled a fault without recording error information.
        sqlstate[0] = '\0';
        if (!msg.Append(DEFAULT_ERROR))
            return 0;
    }

    return GetError(sqlstate, 0, msg.Detach());
}

static bool GetSqlState(HSTMT hstmt, char* szSqlState)
{
    SQLCHAR szMsg[300];
    SQLSMALLINT cbMsg = (SQLSMALLINT)(_countof(szMsg) - 1);
    SQLINTEGER nNative;
    SQLSMALLINT cchMsg;
    SQLRETURN ret;

    Py_BEGIN_ALLOW_THREADS
    ret = SQLGetDiagRec(SQL_HANDLE_STMT, hstmt, 1, (SQLCHAR*)szSqlState, &nNative, szMsg, cbMsg, &cchMsg);
    Py_END_ALLOW_THREADS
    return SQL_SUCCEEDED(ret);
}

bool HasSqlState(HSTMT hstmt, const char* szSqlState)
{
    char szActual[6];
    if (!GetSqlState(hstmt, szActual))
        return false;
    return memcmp(szActual, szSqlState, 5) == 0;
}
