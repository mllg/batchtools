#include <R.h>
#include <Rinternals.h>

SEXP c_binpack(SEXP x_, SEXP order_, SEXP capacity_) {
    const double * x = REAL(x_);
    const int * order = INTEGER(order_);
    const double capacity = REAL(capacity_)[0];
    const R_len_t n = length(x_);

    if (x[order[0] - 1] > capacity)
        error("Capacity not sufficient. Largest item does not fit.");

    SEXP res = PROTECT(allocVector(INTSXP, n));
    int * bin = INTEGER(res);

    double * sums = malloc(n * sizeof(double));
    R_len_t bins = 1;

    for (R_len_t i = 0; i < n; i++) {
        R_len_t ii = order[i] - 1;
        Rboolean assigned = FALSE;
        for (R_len_t pos = 0; pos < bins; pos++) {
            double tmp = sums[pos] + x[ii];
            if (tmp <= capacity) {
                bin[ii] = pos + 1;
                sums[pos] = tmp;
                assigned = TRUE;
                break;
            }
        }
        if (!assigned) {
            bin[ii] = bins + 1;
            sums[bins] = x[ii];
            bins++;
        }
    }

    free(sums);
    UNPROTECT(1);
    return res;
}
