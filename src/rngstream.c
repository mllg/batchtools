#include <R.h>
#include <Rinternals.h>
#include <stdint.h>

typedef uint_least64_t Uint64;

static const Uint64 A1p127[3][3] = {
    {    2427906178, 3580155704,  949770784 },
    {     226153695, 1230515664, 3580155704 },
    {    1988835001,  986791581, 1230515664 }};

static const Uint64 A2p127[3][3] = {
    {    1464411153,  277697599, 1610723613 },
    {      32183930, 1464411153, 1022607788 },
    {    2824425944,   32183930, 2093834863 }};

SEXP next_streams(SEXP x_, SEXP i_, SEXP ord_) {
    Uint64 seed[6], nseed[6], tmp;
    for (int i = 0; i < 6; i++)
        seed[i] = (unsigned int)INTEGER(x_)[i+1];

    const int n = length(i_);
    SEXP ans = PROTECT(allocMatrix(INTSXP, 7, n));
    int ii = INTEGER(i_)[INTEGER(ord_)[0] - 1];

    for (int nstate = 1, k = 0;; nstate++) {
        for (int i = 0; i < 3; i++) {
            tmp = 0;
            for(int j = 0; j < 3; j++) {
                tmp += A1p127[i][j] * seed[j];
                tmp %= 4294967087;
            }
            nseed[i] = tmp;
        }

        for (int i = 0; i < 3; i++) {
            tmp = 0;
            for(int j = 0; j < 3; j++) {
                tmp += A2p127[i][j] * seed[j+3];
                tmp %= 4294944443;
            }
            nseed[i+3] = tmp;
        }

        if (nstate == ii) {
            INTEGER(ans)[k * 7] = INTEGER(x_)[0];
            for (int i = 0;  i < 6; i++)
                INTEGER(ans)[k * 7 + i + 1] = (int) nseed[i];
            if (++k == n)
                break;
            ii = INTEGER(i_)[INTEGER(ord_)[k] - 1];
        }

        for (int i = 0; i < 6; i++)
            seed[i] = nseed[i];
    }

    UNPROTECT(1);
    return ans;
}
