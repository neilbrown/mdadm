
/* Simple wrapper for code in sha1.c, to
 * provide SHA1() interface as provided by opensll.
 * I do this because I cannot get SHA1 when staticly linking.
 *
 * sha1.c sha1.h md5.h all copied from coreutils-5.94
 */

#include "sha1.h"

unsigned char *SHA1(unsigned char *buf, int len, unsigned char *dest)
{
	static unsigned char defdest[20];
	if (dest == NULL) dest = defdest;

	return (unsigned char *)sha1_buffer((const char*)buf,
					    len,
					    (void*)dest);
}
