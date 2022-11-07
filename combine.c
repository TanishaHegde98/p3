#include <assert.h>
#include <fcntl.h>
#include <stdio.h>
#include <stdlib.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <unistd.h>
#include <pthread.h>
#include <string.h>
#include <sys/time.h>

typedef unsigned int uint;
//Store input in key value pairs
typedef struct {
  int key;
  char *value;
} rec_t;
uint size;

//#define swap(a, b) { int _h = a; a = b; b = _h; }
#define min(a, b) ((a) < (b) ? (a) : (b))
void swap(rec_t *a, rec_t *b){
    int key= a->key;
    //temp.key = a->key;
    char * tVal;
    tVal = (char *)malloc(sizeof(char)*100);
    memcpy(tVal,a->value,100);
    //free(a->value);
    a->key=b->key;
    a->value=b->value;
    b->key=key;
   b->value=tVal;
}

//void swaoWithPiv()
void sort3fast(rec_t *a, rec_t *b, rec_t *c){
    if (b->key < a ->key) {
        if (c->key < a->key) { 
            if (c->key < b->key) { swap(a, c); }
            else{
                rec_t temp;
                temp.key = a->key;
                temp.value=(char *)malloc(sizeof(char)*100);
                memcpy(temp.value,a->value,100);
                //free(a->value);
                a->key=b->key;
                a->value=b->value;
                b->key=c->key;
                b->value=c->value;
                c->key=temp.key;
                c->value=temp.value;
            }
        }
        else { swap(a, b); } 
    }
    else{
        if (c < b) {
            if (c < a) { 
                rec_t temp;
                temp.key = c->key;
                temp.value=(char *)malloc(sizeof(char)*100);
                memcpy(temp.value,c->value,100);
                //free(c->value);
                c->key=b->key;
                c->value=b->value;
                b->key=a->key;
                b->value=b->value;
                //b=a;
                a->key=temp.key;
                a->value=temp.value;
                }
        }
        else { swap(b, c); } 
    }
}
int max_threads;
int n_threads;

pthread_mutex_t mutex = PTHREAD_MUTEX_INITIALIZER;
pthread_cond_t cond = PTHREAD_COND_INITIALIZER;


rec_t * read_inp(char * fn, uint size){
    struct {
    int fd;
    char *map;
    char *filename;
  } inp;
  inp.filename = fn;

  if ((inp.fd = open(inp.filename, O_RDONLY)) == -1) {
    perror("Error opening file");
    exit(EXIT_FAILURE);
  }

  if ((inp.map = mmap(0, size, PROT_READ, MAP_SHARED, inp.fd, 0)) ==
          MAP_FAILED)
    exit(EXIT_FAILURE);

  size = size / 100; // read 100 bytes as a seperate record
  rec_t *rec_t_map =
      (rec_t *)malloc(size * sizeof(rec_t));
  rec_t *c = rec_t_map;

// store input in map
  for (char *r = inp.map; r < inp.map + size * 100; r += 100) {
    c->key = *(int *)r;
    //printf("%d ",c->key);
    c->value = r;
    c++;
  }

close(inp.fd);
return rec_t_map;
}

void write_op(char * fn, rec_t * rec_t_map, uint size){
    FILE *fd = fopen(fn, "wb");
    if(fd == NULL)
    {
        printf("Error!");   
        exit(1);             
    }

    
    for (uint i = 0; i < size/100; i++){
        fwrite(rec_t_map[i].value,100,1,fd);
        //fwrite(&rec_t_map[i], sizeof(rec_t), 1, fd);
        //fprintf(rec_t_map[i].value,);
    }
    fclose(fd);
}

double t(void) {

    static double t0;
    struct timeval tv;
    gettimeofday(&tv, NULL);
    double h = t0;
    t0 = tv.tv_sec + tv.tv_usec / 1000000.0;
    return t0 - h;
}

void insert_sort(rec_t *left, rec_t* right) {

    // put minimum to left position, so we can save
    // one inner loop comparsion for insert sort
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
        //free(pi->value);
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
    //free(mid->value);
    mid->key = (left0 + 1)->key;
    mid->value = (left0 + 1)->value;
    sort3fast(left0, &piv, right0);  // check
    (left0+ 1)->key = piv.key;
    //free((left0+ 1)->value);
    (left0+ 1)->value = piv.value;
    rec_t *left, *right;
    #define BSZ 256
    if (right0 - left0 > 2 * BSZ + 3) {

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
                swap(*(ol + i), *(or + i));
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
    *(left0 + 1) = *right;
    right->key = piv.key;
    //free(right->value);
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

void qusort(rec_t* left, rec_t* right);

void* sort_thr(void *arg) {
    rec_t** par = (rec_t**)arg;
    qusort(par[0], par[1]);
    free(arg);
    pthread_mutex_lock(&mutex);
    n_threads -= 1;
    if (n_threads == 0) pthread_cond_signal(&cond);
    pthread_mutex_unlock(&mutex);
    return NULL;
}

void qusort(rec_t* left, rec_t* right) {

    int diff = right-left;
    //printf("%d",diff);
    while (right - left >= 50) {
        rec_t *l, *r;
        partition(left, right, &l, &r, &left, &right);

        if (right - left > 100000 && n_threads < max_threads) {
            // start a new thread - max_threads is a soft limit
            pthread_t thread;
            rec_t** param = malloc(2 * sizeof(rec_t*));
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

void sort(rec_t* data, int len) {

    //printf("in sort");
    int n_cpus = sysconf(_SC_NPROCESSORS_ONLN);
    if (n_cpus > 0) max_threads = n_cpus * 2;
    else max_threads = 8;

    pthread_t thread;
    rec_t** param = malloc(2 * sizeof(rec_t*));
    param[0] = data;
    param[1] = data + len/100 - 1;
    //printf("%d",param[1]->key);
    n_threads = 1;
    pthread_create(&thread, NULL, sort_thr, param);

    pthread_mutex_lock(&mutex);
    pthread_cond_wait(&cond, &mutex);
    pthread_mutex_unlock(&mutex);
}

int main(int argc, char **argv) {
    struct stat st;
    stat(argv[1], &st);
    uint size=st.st_size;
    rec_t *rec_t_map=read_inp(argv[1],size);
    //printing the input data.
    // for (uint i = 0; i < size/100; i++){
    //     printf("\n*Key=%d",rec_t_map[i].key);
    // }
    
    //printf("Sorting %d million records with Quicksort ...\n",size / 1000000);
    //printf("size %d", size);
    int len= size/100;
    t();
    sort(rec_t_map, size);
    // printf("\nRecords after sorting\n");
    // for (uint i = 0; i < size/100; i++){
    //     printf("\n*Key=%d",rec_t_map[i].key);
    // }
    //printf("\n%.2fs\n", t());
    //Write to file
    write_op(argv[2],rec_t_map,size);
    //printf("\n%.2fs\n", t());
    free(rec_t_map);
    exit(0);
}
