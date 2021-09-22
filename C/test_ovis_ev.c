#include <assert.h>
#include <errno.h>
#include <math.h>
#include <pthread.h>
#include <semaphore.h>
#include <string.h>
#include <unistd.h>
#include "ovis_ev/ev.h"
#include "tada.h"
#include "util.h"

#define FMT "c:f:l:u:"

#define TIME_DIFF_THRESHOLD 0.001 /* 1 ms */
#define GENERIC_WORKER_DST	"generic_dst"
#define GENERIC_WORKER_SRC	"generic_src"
#define GENERIC_TEST_TYPE	"generic_test_type"
#define GENERIC_NO_TEST_TYPE	"generic_no_test_type"
#define NOT_WAIT_TYPE		"not_wait_type"

#define POST_WO_TO_ASSERT_NO			1
#define POST_W_CURRENT_TO_ASSERT_NO		2
#define POST_W_FUTURE_TO_ASSERT_NO		3
#define REPOST_POSTED_EVENT_ASSERT_NO		4
#define CANCEL_POSTED_EVENT_ASSERT_NO		5
#define RESCHEDULE_ASSERT_NO			6
#define DELIVER_ORDER_ASSERT_NO			7
#define FLUSH_EVENTS_ASSERT_NO			8
#define POST_EVENT_ON_FLUSHED_WORKER_ASSERT_NO	9
#define MULTI_THREAD_ASSERT_NO			10

sem_t result_ready;

/*
 * 5. flush events from a worker
 * 7. Post the same events multiple times
 */

struct ev_expected_s {
	int assert_no;
	struct test_s *suite;
	ev_type_t ev_type;
	ev_t ev;
	ev_status_t status;

	struct timespec *to;
	char *dst_worker;
};

static int no_test_actor(ev_worker_t src, ev_worker_t dst,
				ev_status_t status, ev_t e)
{
	return 0;
}

static int test_generic_actor(ev_worker_t src, ev_worker_t dst,
					ev_status_t status, ev_t e)
{
	int rc;
	struct ev_expected_s *exp;
	struct timespec recv_time;
	ldms_test_buf_t reason;

	reason = ldms_test_buf_alloc(1024);
	if (!reason)
		assert(0 == ENOMEM);

	ev_sched_to(&recv_time, 0, 0);
	exp = EV_DATA(e, struct ev_expected_s);

	if (exp->status != status) {
		rc = ldms_test_buf_append(reason, "The deliver status '%d' "
				"is not '%d'", status, exp->status);
		assert(0 == rc);
		tada_assert(exp->suite, exp->assert_no, TADA_FALSE, reason->buf);
		goto out;
	}


	if (0 != strcmp(exp->dst_worker, ev_worker_name(dst))) {
		rc = ldms_test_buf_append(reason, "The event is delivered to "
				"a wrong worker '%s' instead of '%s'",
				ev_worker_name(dst), exp->dst_worker);
		assert(rc == 0);
		tada_assert(exp->suite, exp->assert_no, TADA_FALSE, reason->buf);
		goto out;
	}

	if (exp->ev != e) {
		rc = ldms_test_buf_append(reason, "Wrong event is delivered.");
		assert(rc == 0);
		tada_assert(exp->suite, exp->assert_no, TADA_FALSE, reason->buf);
		goto out;
	}

	if (exp->ev_type != ev_type(e)) {
		rc = ldms_test_buf_append(reason, "Wrong event type '%s' was "
				"delivered, expecting '%s'.",
				ev_type(e), exp->ev_type);
		assert(rc == 0);
		tada_assert(exp->suite, exp->assert_no, TADA_FALSE, reason->buf);
		goto out;
	}

	if (exp->to) {
		double diff = ev_time_diff(&recv_time, exp->to);
		if (diff > TIME_DIFF_THRESHOLD) {
			rc = ldms_test_buf_append(reason, "ovis_ev delivered "
					"event too far (%f sec) from "
					"the expected time.", diff);
			assert(rc == 0);
			tada_assert(exp->suite, exp->assert_no, TADA_FALSE, reason->buf);
			goto out;
		}
	}

	rc  = ldms_test_buf_append(reason, "ovis_ev delivered the expected event.");
	assert(rc == 0);
	tada_assert(exp->suite, exp->assert_no, TADA_TRUE, reason->buf);
out:
	ldms_test_buf_free(reason);
	sem_post(&result_ready);
	return 0;
}

static void test_posting_event(struct test_s *suite)
{

	int rc;
	struct ev_expected_s *exp;
	struct timespec to;
	ev_worker_t dst, src;
	ev_type_t test_type, no_test_type;
	ev_t ev_1, ev_2;

	dst = ev_worker_get(GENERIC_WORKER_DST);
	src = ev_worker_get(GENERIC_WORKER_SRC);

	test_type = ev_type_get(GENERIC_TEST_TYPE);
	no_test_type = ev_type_get(GENERIC_NO_TEST_TYPE);

	ev_1 = ev_new(test_type);
	ev_2 = ev_new(no_test_type);

	exp = EV_DATA(ev_1, struct ev_expected_s);
	exp->suite = suite;
	exp->dst_worker = GENERIC_WORKER_DST;
	exp->ev = ev_1;
	exp->ev_type = test_type;
	exp->status = EV_OK;

	/* without timeout */
	exp->to = NULL;
	exp->assert_no = POST_WO_TO_ASSERT_NO;
	ev_post(src, dst, ev_1, NULL);
	sem_wait(&result_ready);

	/* current timeout */
	ev_sched_to(&to, 0, 0);
	exp->to = &to;
	exp->assert_no = POST_W_CURRENT_TO_ASSERT_NO;
	ev_post(src, dst, ev_1, &to);
	sem_wait(&result_ready);

	/* future timeout */
	ev_sched_to(&to, 2, 0);
	exp->to = &to;
	exp->assert_no = POST_W_FUTURE_TO_ASSERT_NO;
	ev_post(src, dst, ev_1, &to);
	sem_wait(&result_ready);

	/* repost posted event */
	ev_sched_to(&to, 2, 0);
	exp = EV_DATA(ev_2, struct ev_expected_s);
	exp->to = NULL;
	exp->assert_no = REPOST_POSTED_EVENT_ASSERT_NO;
	ev_post(src, dst, ev_2, &to);
	rc = ev_post(src, dst, ev_2, NULL);
	if (EBUSY != rc) {
		tada_assert(suite, exp->assert_no, TADA_FALSE,
				"ev_post didn't return EBUSY "
				"when the test posted an already posted event");
	} else {
		tada_assert(suite, exp->assert_no, TADA_TRUE,
				"ev_post returned EBUSY "
				"when posted an already posted event");
	}
}

struct delivered_order_s {
	test_t suite;
	int assert_no;
	int order;
	int last;
};

static int
test_order_actor(ev_worker_t src, ev_worker_t dst, ev_status_t status, ev_t ev)
{
	static int delivered_order = 0;
	static int is_submited_result = 0;
	struct delivered_order_s *exp;

	exp = EV_DATA(ev, struct delivered_order_s);
	if (exp->order != delivered_order) {
		if (!is_submited_result) {
			tada_assert(exp->suite, exp->assert_no, TADA_FALSE,
					"The event delivery order was wrong.");
			is_submited_result = 1;
		}
	} else {
		if (exp->last) {
			tada_assert(exp->suite, exp->assert_no, TADA_TRUE,
				"The event delivery order was correct.");
			sem_post(&result_ready);
		}
	}
	delivered_order++;
	return 0;
}

#define ORDER_NUM_EVENTS 3
static void test_delivery_order(test_t suite)
{
	ev_worker_t order_dst, order_src;
	ev_type_t order_type;
	ev_t ev_list[ORDER_NUM_EVENTS];
	struct delivered_order_s *exp;
	int i;

	order_dst = ev_worker_get(GENERIC_WORKER_DST);
	order_src = ev_worker_get(GENERIC_WORKER_SRC);

	order_type = ev_type_new("order_type", sizeof(struct delivered_order_s));
	ev_dispatch(order_dst, order_type, test_order_actor);

	for (i = 0; i < ORDER_NUM_EVENTS; i++) {
		ev_list[i] = ev_new(order_type);
		exp = EV_DATA(ev_list[i], struct delivered_order_s);
		exp->suite = suite;
		exp->order = i;
		exp->assert_no = DELIVER_ORDER_ASSERT_NO;
		if (i == ORDER_NUM_EVENTS-1)
			exp->last = 1;
		else
			exp->last = 0;
	}

	for (i = 0; i < ORDER_NUM_EVENTS; i++) {
		ev_post(order_src, order_dst, ev_list[i], NULL);
	}

	sem_wait(&result_ready);
}

static void test_cancel_posted_event(test_t suite)
{
	struct timespec to;
	ev_worker_t dst, src;
	ev_type_t ev_type;
	ev_t ev;
	struct ev_expected_s *exp;

	dst = ev_worker_get(GENERIC_WORKER_DST);
	src = ev_worker_get(GENERIC_WORKER_SRC);

	ev_type = ev_type_get(GENERIC_TEST_TYPE);

	ev = ev_new(ev_type);
	exp = EV_DATA(ev, struct ev_expected_s);

	exp->suite = suite;
	exp->assert_no = CANCEL_POSTED_EVENT_ASSERT_NO;
	exp->status = EV_FLUSH;
	exp->dst_worker = GENERIC_WORKER_DST;
	exp->ev = ev;
	exp->ev_type = ev_type;
	exp->to = NULL;

	ev_sched_to(&to, 2, 0);
	ev_post(src, dst, ev, &to);
	sleep(1);
	ev_cancel(ev);
	sem_wait(&result_ready);

	exp->assert_no = RESCHEDULE_ASSERT_NO;
	exp->status = EV_OK;
	ev_post(src, dst, ev, NULL);
	sem_wait(&result_ready);
}

#define FLUSH_NUM_EVENTS 10

struct flush_event_data {
	test_t suite;
	int assert_no;
	int status;
	int last;
	int is_reset;
};

static int
test_flush_actor(ev_worker_t src, ev_worker_t dst, ev_status_t status, ev_t ev)
{
	static int is_posted = 0;
	struct flush_event_data *exp;
	int result, rc;
	ldms_test_buf_t buf;

	buf = ldms_test_buf_alloc(1024);

	exp = EV_DATA(ev, struct flush_event_data);
	if (exp->is_reset)
		is_posted = 0;

	rc = ldms_test_buf_append(buf, "Expected status (%d) == delivered status (%d)",
					exp->status, status);
	assert(0 == rc);
	if (exp->status != status)
		goto post;

	if (exp->last)
		goto post;
	ldms_test_buf_free(buf);
	return 0;

post:
	if (!is_posted) {
		(void) tada_assert(exp->suite, exp->assert_no, exp->status == status, buf->buf);
		sem_post(&result_ready);
		is_posted = 1;
	}
	ldms_test_buf_free(buf);
	return 0;
}

static int test_flush_events(test_t suite)
{
	struct timespec to[FLUSH_NUM_EVENTS];
	ev_worker_t src, dst;
	ev_type_t ev_type;
	ev_t ev_list[FLUSH_NUM_EVENTS];
	struct flush_event_data *exp;
	int i;

	src = ev_worker_get(GENERIC_WORKER_SRC);
	dst = ev_worker_get(GENERIC_WORKER_DST);

	ev_type = ev_type_new("flush_type", sizeof(struct flush_event_data));
	ev_dispatch(dst, ev_type, test_flush_actor);

	for (i = 0; i < FLUSH_NUM_EVENTS; i++) {
		ev_list[i] = ev_new(ev_type);
		exp = EV_DATA(ev_list[i], struct flush_event_data);

		exp->assert_no = FLUSH_EVENTS_ASSERT_NO;
		exp->suite = suite;

		if (i == FLUSH_NUM_EVENTS - 1)
			exp->last = 1;
		else
			exp->last = 0;

		if (i == floor(FLUSH_NUM_EVENTS/2))
			sleep(1);

		if (i < FLUSH_NUM_EVENTS/2) {
			exp->status = EV_OK;
			ev_post(src, dst, ev_list[i], NULL);
		} else {
			exp->status = EV_FLUSH;
			ev_sched_to(&to[i], 2, 0);
			ev_post(src, dst, ev_list[i], &to[i]);
		}
	}

	ev_flush(dst);
	sem_wait(&result_ready);

	/* Post event on flushed worker */
	exp = EV_DATA(ev_list[0], struct flush_event_data);
	exp->status = EV_OK;
	exp->last = 1;
	exp->is_reset = 1;
	exp->assert_no = POST_EVENT_ON_FLUSHED_WORKER_ASSERT_NO;
	ev_post(src, dst, ev_list[0], NULL);
	sem_wait(&result_ready);

	return 0;
}

#define MULTI_THR_NUM 2

struct thread_data {
	int id;
	int result;
	ev_t common_ev;
	ev_worker_t common_dst;
	ev_worker_t common_src;
};

static void *__post_event(void *v)
{
	int rc;
	int exp_rc;
	struct thread_data *data = (struct thread_data *)v;

	if (1 < data->id ) {
		sleep(1);
		exp_rc = EBUSY;
	} else {
		exp_rc = 0;
	}

	rc = ev_post(data->common_src, data->common_dst, data->common_ev, NULL);
	if (exp_rc != rc)
		data->result = rc;
	else
		data->result = TADA_TRUE;
	return NULL;
}

static void test_multiple_threads_post_event(test_t suite)
{
	pthread_t threads[MULTI_THR_NUM];
	int i;
	struct thread_data data[MULTI_THR_NUM];
	ev_t ev;
	ev_type_t type;

	type = ev_type_get(GENERIC_NO_TEST_TYPE);
	ev = ev_new(type);

	for (i = 0; i < MULTI_THR_NUM; i++) {
		data[i].id = i;
		data[i].common_ev = ev;
		data[i].common_dst = ev_worker_get(GENERIC_WORKER_DST);
		data[i].common_src = ev_worker_get(GENERIC_WORKER_SRC);
		pthread_create(&threads[i], NULL, __post_event, &data[i]);
	}

	for (i = 0; i < MULTI_THR_NUM; i++)
		pthread_join(threads[i], NULL);

	for (i = 0; i < MULTI_THR_NUM; i++) {
		if (TADA_FALSE == data[i].result) {
			(void) tada_assert(suite, MULTI_THREAD_ASSERT_NO,
					TADA_FALSE,
					"ev_post didn't return EBUSY "
					"although the event is posted.");
			return;
		}
	}

	(void) tada_assert(suite, MULTI_THREAD_ASSERT_NO, TADA_TRUE,
			"ev_post returned the expected return code.");
}

int main(int argc, char **argv) {
	int op;
	int test_flags = TADA_TEST_F_LOG_RESULT;
	char *log_path = NULL;
	char *commit_id = "";
	char *user = "";
	ev_worker_t gn_dst, gn_src;
	ev_type_t gn_test_type, gn_no_test_type;

	while ((op = getopt(argc, argv, FMT)) != -1) {
		switch (op) {
		case 'c':
			commit_id = strdup(optarg);
			break;
		case 'f':
			test_flags = atoi(optarg);
			break;
		case 'l':
			log_path = strdup(optarg);
			break;
		case 'u':
			user = strdup(optarg);
			break;
		default:
			fprintf(stderr, "Unrecognized cli-option '%s'", optarg);
		};
	}

	gn_dst = ev_worker_new(GENERIC_WORKER_DST, test_generic_actor);
	gn_src = ev_worker_new(GENERIC_WORKER_SRC, test_generic_actor);

	gn_test_type = ev_type_new(GENERIC_TEST_TYPE, sizeof(struct ev_expected_s));
	gn_no_test_type = ev_type_new(GENERIC_NO_TEST_TYPE,
					sizeof(struct ev_expected_s));
	ev_dispatch(gn_dst, gn_no_test_type, no_test_actor);

	TEST_BEGIN("test_ovis_ev", "ovis_ev_test", "FVT", user,
		   commit_id, "Test the ovis_ev library", log_path,
		   test_flags, suite)
	TEST_ASSERTION(suite, POST_WO_TO_ASSERT_NO,
				"Test posting an event without timeout")
	TEST_ASSERTION(suite, POST_W_CURRENT_TO_ASSERT_NO,
				"Test posting an event with a current timeout")
	TEST_ASSERTION(suite, POST_W_FUTURE_TO_ASSERT_NO,
				"Test posting an event with a future timeout")
	TEST_ASSERTION(suite, REPOST_POSTED_EVENT_ASSERT_NO,
				"Test reposting a posted event")
	TEST_ASSERTION(suite, CANCEL_POSTED_EVENT_ASSERT_NO,
				"Test canceling a posted event")
	TEST_ASSERTION(suite, RESCHEDULE_ASSERT_NO,
				"Test rescheduling a posted event")
	TEST_ASSERTION(suite, DELIVER_ORDER_ASSERT_NO,
				"Test event deliver order")
	TEST_ASSERTION(suite, FLUSH_EVENTS_ASSERT_NO,
				"Test flushing events")
	TEST_ASSERTION(suite, POST_EVENT_ON_FLUSHED_WORKER_ASSERT_NO,
				"Test posting event on a flushed worker")
	TEST_ASSERTION(suite, MULTI_THREAD_ASSERT_NO,
				"Test the case that multiple threads post the same event")
	TEST_END(suite);

	TEST_START(suite);
	test_posting_event(&suite);
	test_cancel_posted_event(&suite);
	test_delivery_order(&suite);
	test_flush_events(&suite);
	test_multiple_threads_post_event(&suite);
	TEST_FINISH(suite);

	if (log_path)
		free(log_path);
	return 0;
}

