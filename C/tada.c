#include <stdio.h>
#include <netdb.h>
#include <assert.h>
#include <string.h>
#include <time.h>
#include <unistd.h>
#include <openssl/sha.h>
#include "tada.h"

void _submit(test_t test, char *msg_buf, size_t cnt)
{
	int rc;
	FILE *log = stdout;
	static int line_no = 0;
	if (test->flags & TADA_TEST_F_SEND_RESULT) {
		rc = sendto(test->udp_fd, msg_buf, cnt, 0,
				(struct sockaddr *)&test->sin,
				sizeof(test->sin));
		if (rc < 0) {
			fprintf(stderr, "Failed to send a message to "
					"the TADA server, error %d.\n", rc);
		}
	}

	if (test->flags & TADA_TEST_F_LOG_RESULT) {
		if (test->log_file)
			log = test->log_file;
		if (line_no > 0)
			fprintf(log, ",\n");
		fprintf(log, "%s\n", msg_buf);
	}
	line_no++;
}

/*
 * { "msg-type" : "test-start",
 *   "test-suite" : <suite-name>,
 *   "test-type" : <test-type>,
 *   "test-name" : <test-name>,
 *   "timestamp" : <timestamp>
 * }
 */
void tada_start(test_t test)
{
	size_t cnt;
	char msg_buf[1024];
	char *tada_addr = getenv("TADA_ADDR");
	char *tada_host;
	short tada_port;
	struct hostent *h;
	unsigned char md[32];
	int i;
	time_t ts;
	FILE *f = stdout;

	if (test->flags & TADA_TEST_F_LOG_RESULT) {
		if (test->log_path) {
			test->log_file = fopen(test->log_path, "w");
			if (test->log_file) {
				f = test->log_file;
			} else {
				fprintf(stderr, "Failed to open the log file '%s'. "
						"The log messages are printed to stdout.\n",
						test->log_path);
			}
		}
		fprintf(f, "[");
	}


	if (!tada_addr) {
		tada_host = TADAD_HOST;
		tada_port = htons(TADAD_PORT);
	} else {
		char *s = strdup(tada_addr);
		tada_host = strtok(s, ":");
		tada_port = htons(atoi(strtok(NULL, ":")));
	}
	h = gethostbyname(tada_host);
	assert(h);
	assert (h->h_addrtype == AF_INET);

	ts = time(NULL);

	/* calculate sha256 */
	cnt = snprintf(msg_buf, sizeof(msg_buf),
		 "%s:%s:%s:%s:%s:%ld",
		 test->suite_name,
		 test->test_type,
		 test->test_name,
		 test->test_user,
		 test->commit_id,
		 ts);
	SHA256((unsigned char *)msg_buf, cnt, md);
	for (i = 0; i < 32; i++) {
		snprintf(&test->test_id[i*2], 3, "%02hhx", md[i]);
	}

	test->sin.sin_addr.s_addr = *(unsigned int *)(h->h_addr_list[0]);
	test->sin.sin_family = h->h_addrtype;
	test->sin.sin_port = tada_port;

	cnt = snprintf(msg_buf, sizeof(msg_buf),
		       "{ \"msg-type\" : \"test-start\","
		       "\"test-suite\" : \"%s\","
		       "\"test-type\" : \"%s\","
		       "\"test-name\" : \"%s\","
		       "\"test-user\" : \"%s\","
		       "\"test-desc\" : \"%s\","
		       "\"commit-id\" : \"%s\","
		       "\"timestamp\" : %lu,"
		       "\"test-id\" : \"%s\""
		       "}",
		       test->suite_name,
		       test->test_type,
		       test->test_name,
		       test->test_user,
		       test->test_desc,
		       test->commit_id,
		       ts,
		       test->test_id
		       );
	assert(cnt < sizeof(msg_buf));
	test->udp_fd = socket(AF_INET, SOCK_DGRAM, 0);
	assert(test->udp_fd >= 0);
	_submit(test, msg_buf, cnt);
}

void tada_finish(test_t test)
{
	size_t cnt;
	char msg_buf[1024];
	int assert_no;

	/*
	 * Go through the list of all test assertions and send status
	 * for un-tested assertions.
	 */
	for (assert_no = 0; test->test_asserts[assert_no].test; assert_no++) {
		if (test->test_asserts[assert_no].result != TEST_SKIPPED)
			continue;
		cnt = snprintf(msg_buf, sizeof(msg_buf),
			       "{ \"msg-type\" : \"assert-status\","
			       "\"test-id\" : \"%s\","
			       "\"assert-no\" : %d,"
			       "\"assert-desc\" : \"%s\","
			       "\"assert-cond\" : \"none\","
			       "\"test-status\" : \"skipped\""
			       "}",
			       test->test_id,
			       assert_no,
			       test->test_asserts[assert_no].description
			       );
		assert(cnt < sizeof(msg_buf));
		_submit(test, msg_buf, cnt);
	}

	cnt = snprintf(msg_buf, sizeof(msg_buf),
		       "{ \"msg-type\" : \"test-finish\","
		       "\"test-id\" : \"%s\","
		       "\"timestamp\" : %lu"
		       "}",
		       test->test_id,
		       time(NULL)
		       );
	assert(cnt < sizeof(msg_buf));
	_submit(test, msg_buf, cnt);
	close(test->udp_fd);
	test->udp_fd = -1;
	if (test->log_file) {
		fprintf(test->log_file, "]");
		fclose(test->log_file);
	} else {
		fprintf(stdout, "]");
	}

}

int tada_assert(test_t test, int assert_no, int cond, const char *cond_str)
{
	size_t cnt;
	char msg_buf[1024];
	char esc_str[512];
	const char *s;
	char *e;

	/* Escape any embedded '"' in the cond_str */
	for (s = cond_str, e = esc_str; *s != '\0'; s++) {
		if (*s == '"') {
			*e++ = '\\';
			*e++ = '"';
		} else {
			*e++ = *s;
		}
	}
	*e = '\0';

	assert(test->test_asserts[assert_no].test);
	if (cond)
		test->test_asserts[assert_no].result = TEST_PASSED;
	else
		test->test_asserts[assert_no].result = TEST_FAILED;
	cnt = snprintf(msg_buf, sizeof(msg_buf),
		       "{ \"msg-type\" : \"assert-status\","
		       "\"test-id\" : \"%s\","
		       "\"assert-no\" : %d,"
		       "\"assert-desc\" : \"%s\","
		       "\"assert-cond\" : \"%s\","
		       "\"test-status\" : \"%s\""
		       "}",
		       test->test_id,
		       assert_no,
		       test->test_asserts[assert_no].description,
		       esc_str,
		       test->test_asserts[assert_no].result == TEST_PASSED ? "passed" : "failed"
		       );
	assert(cnt < sizeof(msg_buf));
	_submit(test, msg_buf, cnt);
	return cond;
}
