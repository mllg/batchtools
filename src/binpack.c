#include <R.h>
#include <Rinternals.h>

SEXP c_binpack(SEXP x_, SEXP order_, SEXP capacity_) {
    const double * x = REAL(x_);
    const R_len_t n = length(x_);
    const int * order = INTEGER(order_);
    const double capacity = REAL(capacity_)[0];
    R_len_t ii = order[0] - 1;

    if (x[ii] > capacity)
        error("Capacity not sufficient. Largest item does not fit.");

    SEXP res = PROTECT(allocVector(INTSXP, n));
    int * bin = INTEGER(res);
    double * sums = malloc(n * sizeof(double));
    R_len_t pos, bins = 1;
    bin[ii] = pos + 1;
    sums[0] += x[ii];

    for (R_len_t i = 1; i < n; i++) {
        ii = order[i] - 1;

        for (pos = 0; pos < bins + 1; pos++) {
            if (sums[pos] + x[ii] <= capacity) {
                break;
            }
        }
        bin[ii] = pos + 1;
        sums[pos] += x[ii];
        if (pos > bins)
            bins++;
    }

    free(sums);
    UNPROTECT(1);
    return res;
}
