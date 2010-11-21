
#include "pyodbc.h"

static const char* hexdigits = "0123456789ABCDEF";

void HexDump(const char* name, const void* buffer, int len)
{
    // A debugging utility that prints a buffer to the screen in two columns: hex and ASCII.

    printf("%s (%d)\n", name, len);

    const unsigned char* pb = (const unsigned char *)buffer;
    const unsigned char* pbMax = pb + len;

    char hexstr[10 * 3 + 1];
    char asciistr[10 * 2 + 1];

    while (pb < pbMax)
    {
        memset(hexstr, ' ', sizeof(hexstr));
        memset(asciistr, ' ', sizeof(asciistr));
        hexstr[sizeof(hexstr)-1] = 0;
        asciistr[sizeof(asciistr)-1] = 0;

        for (int i = 0; i < 10 && pb < pbMax; i++, pb++)
        {
            hexstr[(i*3)+0] = hexdigits[(*pb >> 4) & 0x0F];
            hexstr[(i*3)+1] = hexdigits[(*pb) & 0x0F];

            if (isprint(*pb))
                asciistr[(i*2) + 0] = (char)*pb;
            else
                asciistr[(i*2) + 0] = '.';
        }
        printf("%s  %s\n", hexstr, asciistr);
    }
    printf("\n");
}


#ifdef PYODBC_TRACE
void DebugTrace(const char* szFmt, ...)
{
    va_list marker;
    va_start(marker, szFmt);
    vprintf(szFmt, marker);
    va_end(marker);
}
#endif

#ifdef PYODBC_LEAK_CHECK

// THIS IS NOT THREAD SAFE: This is only designed for the single-threaded unit tests!

struct Allocation
{
    const char* filename;
    int lineno;
    size_t len;
    void* pointer;
    int counter;
};

static Allocation* allocs = 0;
static int bufsize = 0;
static int count = 0;
static int allocCounter = 0;

void* _pyodbc_malloc(const char* filename, int lineno, size_t len)
{
    void* p = malloc(len);
    if (p == 0)
        return 0;

    if (count == bufsize)
    {
        allocs = (Allocation*)realloc(allocs, (bufsize + 20) * sizeof(Allocation));
        if (allocs == 0)
        {
            // Yes we just lost the original pointer, but we don't care since everything is about to fail.  This is a
            // debug leak check, not a production malloc that needs to be robust in low memory.
            bufsize = 0;
            count   = 0;
            return 0;
        }
        bufsize += 20;
    }

    allocs[count].filename = filename;
    allocs[count].lineno   = lineno;
    allocs[count].len      = len;
    allocs[count].pointer  = p;
    allocs[count].counter  = allocCounter++;

    printf("malloc(%d): %s(%d) %d %p\n", allocs[count].counter, filename, lineno, (int)len, p);

    count += 1;

    return p;
}

void pyodbc_free(void* p)
{
    if (p == 0)
        return;

    for (int i = 0; i < count; i++)
    {
        if (allocs[i].pointer == p)
        {
            printf("free(%d): %s(%d) %d %p i=%d\n", allocs[i].counter, allocs[i].filename, allocs[i].lineno, (int)allocs[i].len, allocs[i].pointer, i);
            memmove(&allocs[i], &allocs[i + 1], sizeof(Allocation) * (count - i - 1));
            count -= 1;
            free(p);
            return;
        }
    }

    printf("FREE FAILED: %p\n", p);
    free(p);
}

void pyodbc_leak_check()
{
    if (count == 0)
    {
        printf("NO LEAKS\n");
    }
    else
    {
        printf("********************************************************************************\n");
        printf("%d leaks\n", count);
        for (int i = 0; i < count; i++)
            printf("LEAK: %d %s(%d) len=%d\n", allocs[i].counter, allocs[i].filename, allocs[i].lineno, allocs[i].len);
    }
}

#endif
