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


SEXP next_streams(SEXP x_, SEXP i_) {
    const int n = length(i_);
    Uint64 seed[6], nseed[6], tmp;

    /* copy over contents of x_ */
    for (int i = 0; i < 6; i++)
        seed[i] = (unsigned int)INTEGER(x_)[i+1];

    /* determine order of i_ */
    int * ord = malloc(n * sizeof(int));
    R_orderVector1(ord, n, i_, FALSE, FALSE);

    /* allocate output matrix */
    SEXP ans = PROTECT(allocMatrix(INTSXP, 7, n));

    R_len_t needle = INTEGER(i_)[ord[0]];
    R_len_t count = 0;
    for (int nstate = 1;; nstate++) {
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

        if (nstate == needle) {
            /* store state in ans */
            int col = ord[count] * 7;
            INTEGER(ans)[col] = INTEGER(x_)[0];
            for (int i = 0; i < 6; i++)
                INTEGER(ans)[col + i + 1] = (int) nseed[i];

            /* advance to next i */
            if (++count == n)
                break;
            needle = INTEGER(i_)[ord[count]];
        }

        /* copy nseed -> seed */
        for (int i = 0; i < 6; i++)
            seed[i] = nseed[i];
    }

    free(ord);
    UNPROTECT(1);
    return ans;
}
