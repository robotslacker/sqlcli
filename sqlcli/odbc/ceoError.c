//-----------------------------------------------------------------------------
// Error.c
//   Error handling.
//-----------------------------------------------------------------------------

#include "ceoModule.h"

//-----------------------------------------------------------------------------
// ceoError_free()
//   Deallocation routine.
//-----------------------------------------------------------------------------
static void ceoError_free(ceoError *self)
{
    Py_CLEAR(self->message);
    Py_TYPE(self)->tp_free((PyObject*) self);
}


//-----------------------------------------------------------------------------
// ceoError_str()
//   Return a string representation of the error variable.
//-----------------------------------------------------------------------------
static PyObject *ceoError_str(ceoError *self)
{
    if (self->message) {
        Py_INCREF(self->message);
        return self->message;
    }
    return CEO_STR_FROM_ASCII("");
}


//-----------------------------------------------------------------------------
// ceoError_check()
//   Check for an error in the last call and if an error has occurred, raise a
// Python exception.
//-----------------------------------------------------------------------------
int ceoError_check(SQLSMALLINT handleType, SQLHANDLE handle, SQLRETURN rcToCheck, const char *context)
{
    SQLINTEGER      numRecords;
    SQLCHAR         buffer[1024];
    SQLCHAR         buffer2[1024];
    SQLSMALLINT     length;
    SQLRETURN       rc;

    int i;
    
    // handle simple cases
    if (rcToCheck == SQL_SUCCESS || rcToCheck == SQL_SUCCESS_WITH_INFO)
        return 0;
    if (rcToCheck == SQL_INVALID_HANDLE) {
        PyErr_SetString(ceoExceptionDatabaseError, "Invalid handle!");
        return -1;
    }

    memset(buffer, '\0', sizeof(buffer));
    memset(buffer2, '\0', sizeof(buffer2));

    rc = SQLGetDiagField(handleType, handle, 0, SQL_DIAG_NUMBER, &numRecords, 0, 0);
    if (rc != SQL_SUCCESS && rc != SQL_SUCCESS_WITH_INFO) {
        sprintf(buffer, "%s", "cannot get number of diagnostic records");
    } else if (numRecords == 0) {
        sprintf(buffer, "%s", "no diagnostic message text available");
    } 
    else
    {
        for (i = 1; i <= numRecords; i++)
        {
            rc = SQLGetDiagField(handleType, handle, i, SQL_DIAG_MESSAGE_TEXT,
                buffer2, sizeof(buffer2), &length);
            if (length > (SQLSMALLINT)sizeof(buffer2) - 1)
                length = (SQLSMALLINT)sizeof(buffer2) - 1;
            if (rc != SQL_SUCCESS && rc != SQL_SUCCESS_WITH_INFO) {
                sprintf(buffer, "%s", "cannot get diagnostic message text");
                break;
            }
            sprintf(buffer, "%s\n%s", buffer, buffer2);
        }
    }
    ceoError_raiseFromString(SQLCliODBCExceptionError, buffer, "NULL");
    return -1;
}


//-----------------------------------------------------------------------------
// ceoError_raiseFromString()
//   Internal method for raising an exception given a string. Returns -1 as a
// convenience to the caller.
//-----------------------------------------------------------------------------
int ceoError_raiseFromString(PyObject *exceptionType, const char *message,
        const char *context)
{
    ceoError *error;

    error = (ceoError*) ceoPyTypeError.tp_alloc(&ceoPyTypeError, 0);
    if (!error)
        return -1;
    error->context = context;
    error->message = PyUnicode_DecodeASCII(message, strlen(message), NULL);
    if (!error->message) {
        Py_DECREF(error);
        return -1;
    }
    PyErr_SetObject(exceptionType, (PyObject*) error);
    Py_DECREF(error);
    return -1;
}


//-----------------------------------------------------------------------------
// declaration of members for Python type
//-----------------------------------------------------------------------------
static PyMemberDef ceoMembers[] = {
    { "message", T_OBJECT, offsetof(ceoError, message), READONLY },
    { "context", T_STRING, offsetof(ceoError, context), READONLY },
    { NULL }
};


//-----------------------------------------------------------------------------
// declaration of Python type
//-----------------------------------------------------------------------------
PyTypeObject ceoPyTypeError = {
    PyVarObject_HEAD_INIT(NULL, 0)
    .tp_name = "SQLCliODBCException._Error",
    .tp_basicsize = sizeof(ceoError),
    .tp_dealloc = (destructor) ceoError_free,
    .tp_str = (reprfunc) ceoError_str,
    .tp_flags = Py_TPFLAGS_DEFAULT|Py_TPFLAGS_BASETYPE,
    .tp_members = ceoMembers
};
