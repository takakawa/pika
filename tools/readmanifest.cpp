#include <stdio.h>
#include <stdint.h>
#include <unistd.h>
#include <sys/time.h>
#include <random>
#include <stdio.h>
#include <fcntl.h>

struct Version {
	uint32_t  filenum;
        uint64_t  offset;
        uint64_t  logicid;
}__attribute__((packed));

int main(int argc ,char *argv []){

	int fd = open(argv[1], O_RDWR);
       
        Version v;
	
	if (fd != -1 ){
		int n =	read(fd, &v, sizeof(v));
                if (n == sizeof(v)){
			printf(" filenum: %16x = %12d \n offset:  %16llx = %12lld \n logicid: %16llx = %12lld \n", v.filenum,v.filenum, v.offset,v.offset, v.logicid,v.logicid);
		}
        }
	printf(" err");
}
