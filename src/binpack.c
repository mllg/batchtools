#include <R.h>
#include <Rinternals.h>
#include <R_ext/Visibility.h>
#include <stdbool.h>

SEXP attribute_hidden c_binpack(SEXP x_, SEXP order_, SEXP capacity_) {
    const double * x = REAL(x_);
    const R_len_t n = length(x_);
    const int * order = INTEGER(order_);
    const double capacity = REAL(capacity_)[0];
    R_len_t ii = order[0] - 1;
    if (x[ii] > capacity)
        error("Capacity not sufficient. Largest item does not fit.");

    SEXP res = PROTECT(allocVector(INTSXP, n));
    int * bin = INTEGER(res);
    double * capacities = malloc(n * sizeof(double));

    R_len_t bins = 1;
    bin[ii] = 1;
    capacities[0] = capacity - x[ii];

    for (R_len_t i = 1; i < n; i++) {
        ii = order[i] - 1;
        bool packed = false;
        for (R_len_t pos = 0; !packed && pos < bins; pos++) {
            if (capacities[pos] >= x[ii]) {
                packed = true;
                bin[ii] = pos + 1;
                capacities[pos] -= x[ii];
                break;
            }
        }

        if (!packed) {
            capacities[bins] = capacity - x[ii];
            bins++;
            bin[ii] = bins;
        }
    }

    free(capacities);
    UNPROTECT(1);
    return res;
}
