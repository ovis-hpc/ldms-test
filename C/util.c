#include <stdlib.h>
#include <stdio.h>
#include <stdarg.h>
#include <string.h>
#include <errno.h>
#include "util.h"

ldms_test_buf_t ldms_test_buf_alloc(size_t len)
{
	ldms_test_buf_t buf;
	buf = calloc(1, sizeof(*buf));
	if (!buf)
		return NULL;
	buf->buf = malloc(len);
	if (!buf->buf) {
		free(buf);
		return NULL;
	}
	buf->len = len;
	buf->off = 0;
	buf->buf[0] = '\0';
	return buf;
}

void ldms_test_buf_free(ldms_test_buf_t buf)
{
	free(buf->buf);
	free(buf);
}

ldms_test_buf_t ldms_test_buf_realloc(ldms_test_buf_t buf, size_t new_len)
{
	char *_buf;

	_buf = realloc(buf->buf, new_len);
	if (!_buf) {
		ldms_test_buf_free(buf);
		return NULL;
	}

	buf->buf = _buf;
	buf->len = new_len;
	return buf;
}

int ldms_test_buf_append(ldms_test_buf_t buf, const char *fmt, ...)
{
	va_list ap;
	size_t cnt;

retry:
	va_start (ap, fmt);
	cnt = vsnprintf(&buf->buf[buf->off], buf->len - buf->off, fmt, ap);
	va_end(ap);
	if (cnt >= (buf->len - buf->off)) {
		buf = ldms_test_buf_realloc(buf, (buf->len * 2) + cnt);
		if (!buf)
			return ENOMEM;
		goto retry;
	}
	buf->off += cnt;
	return 0;
}

void ldms_test_buf_reset(ldms_test_buf_t buf)
{
	buf->buf[0] = '\0';
	buf->off = 0;
}
