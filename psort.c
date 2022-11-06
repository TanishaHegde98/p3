#include <stdlib.h>
#include <stdio.h>
#include <pthread.h>
#include <fcntl.h>
#include <unistd.h>
#include <string.h>
#include <sys/time.h>

#define SIZE (50 * 1000000)
#define swap(a, b) { int _h = a; a = b; b = _h; }
#define min(a, b) ((a) < (b) ? (a) : (b))

#define sort3fast(a, b, c)              \
    if (b < a) {                        \
        if (c < a) {                    \
            if (c < b) { swap(a, c); }  \
            else { int h = a; a = b; b = c; c = h;} \
        }                               \
        else { swap(a, b); }            \
    }                                   \
    else {                              \
        if (c < b) {                    \
            if (c < a) { int h = c; c = b; b = a; a = h;} \
            else { swap(b, c); }        \
        }                               \
    }  

int data[SIZE];
int max_threads;
int n_threads;

pthread_mutex_t mutex = PTHREAD_MUTEX_INITIALIZER;
pthread_cond_t cond = PTHREAD_COND_INITIALIZER;


void init(int* data, int len) {

    for (int i = 0; i < len; i++) {
        data[i] = rand();
    }
}

double t(void) {

    static double t0;
    struct timeval tv;
    gettimeofday(&tv, NULL);
    double h = t0;
    t0 = tv.tv_sec + tv.tv_usec / 1000000.0;
    return t0 - h;
}

void insert_sort(int* left, int* right) {

    // put minimum to left position, so we can save
    // one inner loop comparsion for insert sort
    for (int* pi = left + 1; pi <= right; pi++) {
        if (*pi < *left) {
            swap(*pi, *left);
        }
    }
    for (int* pi = left + 2; pi <= right; pi++) {
        int h = *pi;
        int* pj = pi - 1;
        while (h < *pj) {
            *(pj + 1) = *pj;
            pj -= 1;
        }
        *(pj + 1) = h;
    }
}

void partition(int* left0, int* right0, int** l1, int** r1, int** l2, int** r2) {

    int* mid = left0 + (right0 - left0) / 2;    
    int piv = *mid;
    *mid = *(left0 + 1);
    sort3fast(*left0, piv, *right0);
    *(left0 + 1) = piv;

    int *left, *right;
    #define BSZ 256
    if (right0 - left0 > 2 * BSZ + 3) {

        left = left0 + 2;
        right = right0 - 1;
        int* offl[BSZ];
        int* offr[BSZ];
        int** ol = offl;
        int** or = offr;
        do {
            if (ol == offl) {
                int* pd = left;
                do {
                    *ol = pd;
                    ol += (piv < *pd);
                    pd += 1;
                }
                while (pd < left + BSZ);
            }
            if (or == offr) {
                int* pd = right;
                do {
                    *or = pd;
                    or += (piv > *pd);
                    pd -= 1;    
                }
                while (pd > right - BSZ);
            }
            int min = min(ol - offl, or - offr);
            ol -= min;
            or -= min;
            for (int i = 0; i < min; i++) {
                swap(**(ol + i), **(or + i));
            }
            if (ol == offl) left += BSZ;
            if (or == offr) right -= BSZ;
        }
        while (right - left > 2 * BSZ);
        left -= 1;
        right += 1;
    }
    else {
        left = left0 + 1;
        right = right0;
    }
    while (1) {
        do left += 1; while(*left < piv);
        do right -= 1; while (*right > piv);
        if (left >= right) break;
        swap(*left, *right);
    }
    *(left0 + 1) = *right;
    *right = piv;

    if (right < mid) {
        *l1 = left0; *r1 = right - 1;
        *l2 = right + 1; *r2 = right0;
    }
    else {
        *l1 = right + 1; *r1 = right0;
        *l2 = left0; *r2 = right - 1;
    }
}

void qusort(int* left, int* right);

void* sort_thr(void *arg) {
    int** par = (int**)arg;
    qusort(par[0], par[1]);
    free(arg);
    pthread_mutex_lock(&mutex);
    n_threads -= 1;
    if (n_threads == 0) pthread_cond_signal(&cond);
    pthread_mutex_unlock(&mutex);
    return NULL;
}

void qusort(int* left, int* right) {

    while (right - left >= 50) {
        int *l, *r;
        partition(left, right, &l, &r, &left, &right);

        if (right - left > 100000 && n_threads < max_threads) {
            // start a new thread - max_threads is a soft limit
            pthread_t thread;
            int** param = malloc(2 * sizeof(int*));
            param[0] = left;
            param[1] = right;
            pthread_mutex_lock(&mutex);
            n_threads += 1;
            pthread_mutex_unlock(&mutex);
            pthread_create(&thread, NULL, sort_thr, param);
            left = l;
            right = r;
        }
        else {
            qusort(l, r);
        }
    }
    insert_sort(left, right);
}

void sort(int* data, int len) {

    
    int n_cpus = sysconf(_SC_NPROCESSORS_ONLN);
    if (n_cpus > 0) max_threads = n_cpus * 2;
    else max_threads = 8;

    pthread_t thread;
    int** param = malloc(2 * sizeof(int*));
    param[0] = data;
    param[1] = data + len - 1;
    n_threads = 1;
    pthread_create(&thread, NULL, sort_thr, param);

    pthread_mutex_lock(&mutex);
    pthread_cond_wait(&cond, &mutex);
    pthread_mutex_unlock(&mutex);
}


int main(void) {

    init(data, SIZE);
    printf("Sorting %d million numbers with Quicksort ...\n",
        SIZE / 1000000);
    t();
    sort(data, SIZE);
    printf("%.2fs\n", t());
    //test(data, SIZE);
    return 0;
}