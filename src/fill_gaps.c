#include <R.h>
#include <Rinternals.h>

/* similar to last observation carried forward, but resets to NA if the last observation is spotted again */
/* used in log file reading: jobs have a start and stop marker, the lines in between belong to the job */
SEXP fill_gaps(SEXP x) {
    const R_len_t n = length(x);
    int last = NA_INTEGER;

    const int *xi = INTEGER(x);
    const int * const xend = xi + n;

    SEXP y = PROTECT(allocVector(INTSXP, n));
    int *yi = INTEGER(y);


    for(; xi != xend; xi++, yi++) {
        if (*xi == NA_INTEGER) {
            *yi = last;
        } else {
            *yi = *xi;
            last = (*xi == last) ? NA_INTEGER : *xi;
        }
    }

    UNPROTECT(1);
    return y;
}
