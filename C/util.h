#ifndef C_UTIL_H_
#define C_UTIL_H_

#include <stdlib.h>

typedef struct ldms_test_buf {
	char *buf;
	size_t len;
	size_t off;
} *ldms_test_buf_t;

/*
 * \brief Allocate a buffer of size \c len
 */
ldms_test_buf_t ldms_test_buf_alloc(size_t len);
ldms_test_buf_t ldms_test_buf_realloc(ldms_test_buf_t buf, size_t new_len);
void ldms_test_buf_free(ldms_test_buf_t buf);
/*
 * \brief Append the string to the buffer.
 *
 * The buffer is automatically extended if the space is not enough.
 *
 * \return 0 on success. Otherwise, an errno is returned.
 */
int ldms_test_buf_append(ldms_test_buf_t buf, const char *fmt, ...);
/*
 * \brief Clear the content in \c buf
 */
void ldms_test_buf_reset(ldms_test_buf_t buf);

#endif /* C_UTIL_H_ */
