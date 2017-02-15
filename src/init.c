#include <R.h>
#include <Rinternals.h>
#include <stdlib.h> // for NULL
#include <R_ext/Rdynload.h>

/* .Call calls */
extern SEXP c_binpack(SEXP, SEXP, SEXP);
extern SEXP c_lpt(SEXP, SEXP, SEXP);
extern SEXP count_not_missing(SEXP);
extern SEXP fill_gaps(SEXP);

static const R_CallMethodDef CallEntries[] = {
    {"c_binpack",         (DL_FUNC) &c_binpack,         3},
    {"c_lpt",             (DL_FUNC) &c_lpt,             3},
    {"count_not_missing", (DL_FUNC) &count_not_missing, 1},
    {"fill_gaps",         (DL_FUNC) &fill_gaps,         1},
    {NULL, NULL, 0}
};

void R_init_batchtools(DllInfo *dll) {
    R_registerRoutines(dll, NULL, CallEntries, NULL, NULL);
    R_useDynamicSymbols(dll, FALSE);
}
