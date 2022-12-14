#include <assert.h>
#include <fcntl.h>
#include <stdio.h>
#include <stdlib.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <unistd.h>
#include <pthread.h>
#include <string.h>

typedef unsigned int uint;
//Store input in key value pairs
typedef struct {
  int key;
  char *value;
} rec_t;
uint size;

int max_threads;
int n_threads;

#define BSZ 64*(sizeof(rec_t))
#define min(a, b) ((a) < (b) ? (a) : (b))
#define swap(a, b) { rec_t _h = *a; *a = *b; *b = _h; }

#define sortfast(a, b, c)              \
    if ((*b).key < a->key) {                        \
        if (c->key < a->key) {                    \
            if (c->key < (*b).key) { swap(a, c); }  \
            else { rec_t h = *a; *a = *b; *b = *c; *c = h;} \
        }                               \
        else { swap(a, b); }            \
    }                                   \
    else {                              \
        if (c->key < (*b).key) {                    \
            if (c->key < a->key) { rec_t h = *c; *c = *b; *b = *a; *a = h;} \
            else { swap(b, c); }        \
        }                               \
    }   \

pthread_mutex_t mutex = PTHREAD_MUTEX_INITIALIZER;
pthread_cond_t cond = PTHREAD_COND_INITIALIZER;


rec_t * read_inp(char * fn, uint size){
    struct {
    int fd;
    char *map;
    char *filename;
  } inp;
  inp.filename = fn;

  if ((inp.fd = open(inp.filename, O_RDONLY)) < 0) {
    fprintf(stderr,"%s","An error has occurred\n");
    exit(0);
  }

  if ((inp.map = mmap(0, size, PROT_READ, MAP_SHARED, inp.fd, 0)) ==
          MAP_FAILED)
    {
        fprintf(stderr,"%s","An error has occurred\n");
        exit(0);
    }

  size = size / 100; // read 100 bytes as a seperate record
  rec_t *rec_t_map =
      (rec_t *)malloc(size * sizeof(rec_t));
  rec_t *c = rec_t_map;

// store input in map
  for (char *r = inp.map; r < inp.map + size * 100; r += 100) {
    c->key = *(int *)r;
    c->value = r;
    c++;
  }

close(inp.fd);
return rec_t_map;
}

int write_op(char * fn, rec_t * rec_t_map, uint size){
    int rc=0;
    int fd = open(fn, O_CREAT | O_WRONLY | O_TRUNC, 0666);
    if (fd < 0) {
        perror("An error has occured");
        exit(1);
    }
    for (uint i = 0; i < size/100; i++){
        rc = write(fd, rec_t_map[i].value, 100);
    }
    fsync(fd);
    close(fd);
    return rc;
}
void insert_records(rec_t *left, rec_t* right) {

    // Do an insertion sort to put all records less than pivot to left
    // records more than pivot to right of pivot
    for (rec_t * pi = left + 1; pi <= right; pi++) {
        if (pi->key < left->key) {
            swap(pi, left);
        }
    }
    for (rec_t * pi = left + 2; pi <= right; pi++) {
        rec_t h;
        h.key = pi->key;
        h.value=(char *)malloc(sizeof(char)*100);
        memcpy(h.value,pi->value,100);
        rec_t* pj = pi - 1;
        while (h.key < pj->key) {
            (pj + 1)->key = pj->key;
            (pj + 1)->value=pj->value;
            pj -= 1;
        }
        (pj + 1)->key = h.key;
        (pj+1)->value=h.value;
    }
}

void partition(rec_t* left0, rec_t* right0, rec_t** l1, rec_t** r1, rec_t** l2, rec_t** r2) {

    rec_t *mid = left0 + (right0 - left0) / 2; 
    rec_t piv;
    piv.key = mid->key;
    piv.value=(char *)malloc(sizeof(char)*100);
    memcpy(piv.value,mid->value,100);
    mid->key = (left0 + 1)->key;
    mid->value = (left0 + 1)->value;
    sortfast(left0, &piv, right0);  // check
    (left0+ 1)->key = piv.key;
    (left0+ 1)->value = piv.value;
    rec_t *left, *right;
    if (right0 - left0 > 2 * BSZ) {

        left = left0 + 2;
        right = right0 - 1;
        rec_t* offl[BSZ];
        rec_t* offr[BSZ];
        rec_t** ol = offl;
        rec_t** or = offr;
        do {
            if (ol == offl) {
                rec_t* pd = left;
                do {
                    *ol = pd;
                    ol += (piv.key < pd->key); 
                    pd += 1;
                }
                while (pd < left + BSZ);
            }
            if (or == offr) {
                rec_t* pd = right;
                do {
                    *or = pd;
                    or += (piv.key > pd->key);
                    pd -= 1;    
                }
                while (pd > right - BSZ);
            }
            int min = min(ol - offl, or - offr); //Recheck
            ol -= min;
            or -= min;
            for (int i = 0; i < min; i++) {
                swap(*(ol + i),*(or + i));
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
        do left += 1; while(left->key < piv.key);
        do right -= 1; while (right->key > piv.key);
        if (left >= right) break;
        swap(left, right);
    }
    (left0 + 1)->key = right->key;
    (left0 + 1)->value = right->value;
    right->key = piv.key;
    right->value=piv.value;

    if (right < mid) {
        *l1 = left0; *r1 = right - 1;
        *l2 = right + 1; *r2 = right0;
    }
    else {
        *l1 = right + 1; *r1 = right0;
        *l2 = left0; *r2 = right - 1;
    }
}

void myQsort(rec_t* left, rec_t* right);

void* thread_sort(void *arg) {
    rec_t** par = (rec_t**)arg;
    myQsort(par[0], par[1]);
    free(arg);
    pthread_mutex_lock(&mutex);
    n_threads -= 1;
    if (n_threads == 0) pthread_cond_signal(&cond);
    pthread_mutex_unlock(&mutex);
    return NULL;
}

void myQsort(rec_t* left, rec_t* right) {

    while (right - left >= 50) {
        rec_t *l, *r;
        partition(left, right, &l, &r, &left, &right);

        if (right - left > 100000 && n_threads < max_threads) {
            // start a new thread only of length more than 100k
            pthread_t thread;
            rec_t** param = malloc(2 * sizeof(rec_t*));
            param[0] = left;
            param[1] = right;
            pthread_mutex_lock(&mutex);
            n_threads += 1;
            pthread_mutex_unlock(&mutex);
            pthread_create(&thread, NULL, thread_sort, param);
            left = l;
            right = r;
        }
        else {
            myQsort(l, r);
        }
    }
    insert_records(left, right);
}

void sort(rec_t* data, int len) {
    if(len <=100){
        return;
    }
    int n_cpus = sysconf(_SC_NPROCESSORS_ONLN);
    if (n_cpus > 0) max_threads = n_cpus * 2;
    else max_threads = 8;

    pthread_t thread;
    rec_t** param = malloc(2 * sizeof(rec_t*));
    param[0] = data;
    param[1] = data + len/100 - 1;
    n_threads = 1;
    pthread_create(&thread, NULL, thread_sort, param);
    pthread_mutex_lock(&mutex);
    pthread_cond_wait(&cond, &mutex);
    pthread_mutex_unlock(&mutex);
}

int main(int argc, char **argv) {
    struct stat st;
    if (stat(argv[1], &st) < 0)
    {
        fprintf(stderr,"%s","An error has occurred\n");
        exit(0);
        }
    uint size=st.st_size;
    if(size<=0){
        fprintf(stderr,"%s","An error has occurred\n");
        exit(0); 
    }
    rec_t *rec_t_map=read_inp(argv[1],size);
    sort(rec_t_map, size);
    //Write to file
    write_op(argv[2],rec_t_map,size);
    free(rec_t_map);
    exit(0);
}
