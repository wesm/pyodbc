
/*
 * Permission is hereby granted, free of charge, to any person obtaining a copy of this software and associated
 * documentation files (the "Software"), to deal in the Software without restriction, including without limitation the
 * rights to use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies of the Software, and to
 * permit persons to whom the Software is furnished to do so.
 * 
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE
 * WARRANTIES OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS
 * OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR
 * OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
 */

#ifndef ROW_H
#define ROW_H

struct Row
{
    // A Row must act like a sequence (a tuple of results) to meet the DB API specification, but we also allow values
    // to be accessed via lowercased column names.  We also supply a `columns` attribute which returns the list of
    // column names.

    PyObject_HEAD

    // cursor.description, accessed as _description
    PyObject* description;

    // A Python dictionary mapping from column name to a PyInteger, used to access columns by name.
    PyObject* map_name_to_index;

    // The number of values in apValues.
    Py_ssize_t cValues;

    // The column values, stored as an array.
    PyObject** apValues;
};

/*
 * Used to make a new row.  Each row value will be zero and will need a PyObject pointer assigned.
 */
Row* Row_New(PyObject* description, PyObject* map_name_to_index, Py_ssize_t cValues);

void Row_Del(Row* self);

extern PyTypeObject RowType;
#define Row_Check(op) PyObject_TypeCheck(op, &RowType)
#define Row_CheckExact(op) ((op)->ob_type == &RowType)

#endif

