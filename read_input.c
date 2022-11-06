#include <assert.h>
#include <fcntl.h>
#include <stdio.h>
#include <stdlib.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <unistd.h>

typedef unsigned int uint;
//Store input in key value pairs
typedef struct {
  int key;
  char *value;
} rec_t;
uint size;
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

//   struct stat st;
//   stat(inp.filename, &st);
//   uint recsize = st.st_size;

  if ((inp.map = mmap(0, size, PROT_READ, MAP_SHARED, inp.fd, 0)) ==
          MAP_FAILED)
    exit(EXIT_FAILURE);

//   uint size = recsize;
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

//Print input that is read from map
// for (uint i = 0; i < size * 100; i += 100){
//     // printf("\nKey=%d Value=%s\n",rec_t_map[i / 100].key,rec_t_map[i / 100].value);
//     printf("\nKey=%d",rec_t_map[i / 100].key);
// }
  

close(inp.fd);

return rec_t_map;

}


int main(int argc, char **argv) {
    struct stat st;
    stat(argv[1], &st);
    uint size=st.st_size;
    rec_t *rec_t_map=read_inp(argv[1],size);
    //printing the input data.
    for (uint i = 0; i < size; i += 100){
    // printf("\nKey=%d Value=%s\n",rec_t_map[i / 100].key,rec_t_map[i / 100].value);
        printf("\n*Key=%d",rec_t_map[i / 100].key);
    }
    free(rec_t_map);
    exit(0);
}
