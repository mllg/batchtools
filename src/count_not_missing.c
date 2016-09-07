#include <R.h>
#include <Rinternals.h>

static R_len_t count_not_missing_logical(SEXP x) {
    const int * xp = LOGICAL(x);
    const int * const xe = xp + length(x);
    R_len_t count = 0;
    for (; xp != xe; xp++) {
        if (*xp != NA_LOGICAL)
            count++;
    }
    return count;
}

static R_len_t count_not_missing_integer(SEXP x) {
    const int * xp = INTEGER(x);
    const int * const xe = xp + length(x);
    R_len_t count = 0;
    for (; xp != xe; xp++) {
        if (*xp != NA_INTEGER)
            count++;
    }
    return count;
}

static R_len_t count_not_missing_double(SEXP x) {
    const double * xp = REAL(x);
    const double * const xe = xp + length(x);
    R_len_t count = 0;
    for (; xp != xe; xp++) {
        if (!ISNAN(*xp))
            count++;
    }
    return count;
}

static R_len_t count_not_missing_string(SEXP x) {
    const R_len_t nx = length(x);
    R_len_t count = 0;
    for (R_len_t i = 0; i < nx; i++) {
        if (STRING_ELT(x, i) != NA_STRING)
            count++;
    }
    return count;
}

static R_len_t count_not_missing_list(SEXP x) {
    const R_len_t nx = length(x);
    R_len_t count = 0;
    for (R_len_t i = 0; i < nx; i++) {
        if (!isNull(VECTOR_ELT(x, i)))
            count++;
    }
    return count;
}

SEXP count_not_missing(SEXP x) {
    switch(TYPEOF(x)) {
        case LGLSXP:  return ScalarInteger(count_not_missing_logical(x));
        case INTSXP:  return ScalarInteger(count_not_missing_integer(x));
        case REALSXP: return ScalarInteger(count_not_missing_double(x));
        case STRSXP:  return ScalarInteger(count_not_missing_string(x));
        case VECSXP:  return ScalarInteger(count_not_missing_list(x));
        case NILSXP:  return ScalarInteger(0);
        default: error("Object of type '%s' not supported", type2char(TYPEOF(x)));
    }
}
