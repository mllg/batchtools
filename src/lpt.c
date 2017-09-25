#include <R.h>
#include <Rinternals.h>
#include <R_ext/Visibility.h>
#define min(a, b) (((a) < (b)) ? (a) : (b))

SEXP attribute_hidden c_lpt(SEXP x_, SEXP order_, SEXP chunks_) {
    const double * x = REAL(x_);
    const R_len_t n = length(x_);
    const int * order = INTEGER(order_);
    const int chunks = min(INTEGER(chunks_)[0], n);

    SEXP res = PROTECT(allocVector(INTSXP, n));
    int * bin = INTEGER(res);
    double * sums = malloc(chunks * sizeof(double));

    for (R_len_t i = 0; i < chunks; i++) {
        R_len_t ii = order[i] - 1;
        bin[ii] = i + 1;
        sums[i] = x[ii];
    }

    for (R_len_t i = chunks; i < n; i++) {
        R_len_t ii = order[i] - 1;
        R_len_t pos = 0;
        for (R_len_t j = 1; j < chunks; j++) {
            if (sums[j] < sums[pos])
                pos = j;
        }

        bin[ii] = pos + 1;
        sums[pos] += x[ii];
    }


    free(sums);
    UNPROTECT(1);
    return res;
}
