#include <assert.h>
#include <inttypes.h>
#include <pthread.h>
#include <stdlib.h>
#include <stdio.h>
#include <string.h>

#include "ldms/ldms.h"
#include "ldms/ldmsd.h"
#include "ovis_util/util.h"
#include "ovis_log/ovis_log.h"

#define STORE_NAME "test_store"
#define DATA_SUFFIX ".data"
#define HEADER_SUFFIX ".header"

#define LOG_(level, ...) do {\
	ovis_log(mylog, level, ##__VA_ARGS__); \
} while(0);

typedef struct test_store_inst_s {
	char *key;
	FILE *file;
	pthread_mutex_t lock;
	LIST_ENTRY(test_store_inst_s) entry;
} *test_store_inst_t;

static LIST_HEAD(test_store_handle_list, test_store_inst_s) test_store_handle_list;

static pthread_mutex_t cfg_lock;
static char *root_path;

static ovis_log_t mylog;

static char *create_key(const char *container, const char *schema)
{
	size_t len = strlen(container) + strlen(schema) + 2; /* : and \0 */
	char *key = malloc(len);
	if (!key)
		return NULL;
	snprintf(key, len, "%s:%s", container, schema);
	return key;
}

/* The caller must hold the cfg_lock. */
static test_store_inst_t find_inst(const char *key)
{
	struct test_store_inst_s *inst;
	LIST_FOREACH(inst, &test_store_handle_list, entry) {
		if (0 == strcmp(inst->key, key)) {
			return inst;
		}
	}
	return NULL;
}

static int print_header(const char *header_path,
		struct ldmsd_strgp_metric_list *metric_list)
{
	struct ldmsd_strgp_metric *m;
	FILE *f;
	f = fopen(header_path, "w");
	if(!f)
		return errno;
	TAILQ_FOREACH(m, metric_list, entry) {
		fprintf(f, "%s,%s,%d\n", m->name,
				ldms_metric_type_to_str(m->type), m->flags);
	}
	fclose(f);
	return 0;
}

static void free_inst(test_store_inst_t inst)
{
	if (inst->file)
		fclose(inst->file);
	if (inst->key)
		free(inst->key);
	pthread_mutex_destroy(&inst->lock);
	free(inst);
}

/* -------------- COMMON API ------------------ */

static const char *usage(ldmsd_plug_handle_t handle)
{
	return  "    A store plugin for testing in the TADA framework\n"
		"  config name=test_store path=<path>\n"
		"    path     The path to the storage\n";
}

static int config(ldmsd_plug_handle_t handle, struct attr_value_list *kwl,
					     struct attr_value_list *avl)
{
	char *v;
	v = av_value(avl, "path");
	if (!v)
		return EINVAL;
	pthread_mutex_lock(&cfg_lock);
	if (root_path)
		free(root_path);
	root_path = strdup(v);
	pthread_mutex_unlock(&cfg_lock);
	if (!root_path)
		return ENOMEM;
	return 0;
}

static void close_store(ldmsd_plug_handle_t handle, ldmsd_store_handle_t sh)
{
	test_store_inst_t inst = sh;
	if (!sh)
		return;

	pthread_mutex_lock(&cfg_lock);
	LIST_REMOVE(inst, entry);
	pthread_mutex_unlock(&cfg_lock);
	free_inst(inst);
}

static void term(ldmsd_plug_handle_t handle)
{
	test_store_inst_t inst;

	inst = LIST_FIRST(&test_store_handle_list);
	while (inst) {
		LIST_REMOVE(inst, entry);
		free_inst(inst);
		inst = LIST_FIRST(&test_store_handle_list);
	}
}

static ldmsd_store_handle_t
open_store(ldmsd_plug_handle_t handle, const char *container, const char *schema,
	   struct ldmsd_strgp_metric_list *metric_list)
{
	int rc;
	char *key, *common_path, *header_path, *data_path;
	size_t len, hdr_len, data_len;
	struct test_store_inst_s *inst = NULL;

	key = header_path = data_path = common_path = NULL;

	key = create_key(container, schema);
	if (!key)
		return NULL;

	len = strlen(root_path) + strlen(key) + 2;
	common_path = malloc(len);
	if (!common_path)
		goto err;
	snprintf(common_path, len, "%s/%s", root_path, container);
	f_mkdir_p(common_path, 0770);

	hdr_len = len + strlen(HEADER_SUFFIX);
	data_len = len + strlen(DATA_SUFFIX);
	header_path = malloc(hdr_len);
	if (!header_path)
		goto err;
	snprintf(header_path, hdr_len, "%s/%s%s",
			common_path, schema, HEADER_SUFFIX);
	data_path = malloc(data_len);
	if (!data_path)
		goto err;
	snprintf(data_path, data_len, "%s/%s%s",
			common_path, schema, DATA_SUFFIX);

	pthread_mutex_lock(&cfg_lock);
	inst = find_inst(key);
	if (!inst) {
		inst = calloc(1, sizeof(*inst));
		if (!inst)
			goto err;
		inst->key = key;
		inst->file = fopen(data_path, "w");
		if (!inst->file)
			goto err;
		rc = print_header(header_path, metric_list);
		if (rc)
			goto err;
		pthread_mutex_init(&inst->lock, NULL);
		LIST_INSERT_HEAD(&test_store_handle_list, inst, entry);
	}
	pthread_mutex_unlock(&cfg_lock);
	return inst;

err:
	pthread_mutex_unlock(&cfg_lock);
	if (key)
		free(key);
	if (common_path)
		free(common_path);
	if (header_path)
		free(header_path);
	if (data_path)
		free(data_path);
	if (inst)
		free_inst(inst);
	return NULL;
}

#define METRIC_DELIM ","
#define ELE_DELIM ";"

static int
store(ldmsd_plug_handle_t handle, ldmsd_store_handle_t sh,
      ldms_set_t set, int *metric_arry, size_t metric_count)
{
	test_store_inst_t inst = sh;
	struct ldms_timestamp ts = ldms_transaction_timestamp_get(set);
	int i, compid_idx, n;
	enum ldms_value_type metric_type;
	ldms_mval_t val;

	compid_idx = ldms_metric_by_name(set, LDMSD_COMPID);

	pthread_mutex_lock(&inst->lock);
	fprintf(inst->file, "%"PRIu32".%06"PRIu32 "%s",
			ts.sec, ts.usec, METRIC_DELIM);
	fprintf(inst->file, "%s%s",
			ldms_set_producer_name_get(set), METRIC_DELIM);
	fprintf(inst->file, "%"PRIu64, ldms_metric_get_u64(set, compid_idx));
	for (i = 0; i < metric_count; i++) {
		if (compid_idx == metric_arry[i])
			continue;
		fprintf(inst->file, "%s", METRIC_DELIM);

		metric_type = ldms_metric_type_get(set, metric_arry[i]);
		val = ldms_metric_get(set, metric_arry[i]);
		n = ldms_metric_array_get_len(set, metric_arry[i]);

		switch (metric_type) {
		case LDMS_V_CHAR_ARRAY:
			fprintf(inst->file, "\"%s\"", val->a_char);
			break;
		case LDMS_V_CHAR:
			fprintf(inst->file, "'%c'", val->v_char);
			break;
		case LDMS_V_U8:
			fprintf(inst->file, "%hhu", val->v_u8);
			break;
		case LDMS_V_U8_ARRAY:
			for (i = 0; i < n; i++) {
				if (i)
					fprintf(inst->file, "%s", ELE_DELIM);
				fprintf(inst->file, "0x%02hhx", val->a_u8[i]);
			}
			break;
		case LDMS_V_S8:
			fprintf(inst->file, "%18hhd", val->v_s8);
			break;
		case LDMS_V_S8_ARRAY:
			for (i = 0; i < n; i++) {
				if (i)
					fprintf(inst->file, "%s", ELE_DELIM);
				fprintf(inst->file, "%hhd", val->a_s8[i]);
			}
			break;
		case LDMS_V_U16:
			fprintf(inst->file, "%hu", val->v_u16);
			break;
		case LDMS_V_U16_ARRAY:
			for (i = 0; i < n; i++) {
				if (i)
					fprintf(inst->file, "%s", ELE_DELIM);
				fprintf(inst->file, "%hu", val->a_u16[i]);
			}
			break;
		case LDMS_V_S16:
			fprintf(inst->file, "%hd", val->v_s16);
			break;
		case LDMS_V_S16_ARRAY:
			for (i = 0; i < n; i++) {
				if (i)
					fprintf(inst->file, "%s", ELE_DELIM);
				fprintf(inst->file, "%hd", val->a_s16[i]);
			}
			break;
		case LDMS_V_U32:
			fprintf(inst->file, "%u", val->v_u32);
			break;
		case LDMS_V_U32_ARRAY:
			for (i = 0; i < n; i++) {
				if (i)
					fprintf(inst->file, "%s", ELE_DELIM);
				fprintf(inst->file, "%u", val->a_u32[i]);
			}
			break;
		case LDMS_V_S32:
			fprintf(inst->file, "%d", val->v_s32);
			break;
		case LDMS_V_S32_ARRAY:
			for (i = 0; i < n; i++) {
				if (i)
					fprintf(inst->file, "%s", ELE_DELIM);
				fprintf(inst->file, "%d", val->a_s32[i]);
			}
			break;
		case LDMS_V_U64:
			fprintf(inst->file, "%"PRIu64, val->v_u64);
			break;
		case LDMS_V_U64_ARRAY:
			for (i = 0; i < n; i++) {
				if (i)
					fprintf(inst->file, "%s", ELE_DELIM);
				fprintf(inst->file, "%"PRIu64, val->a_u64[i]);
			}
			break;
		case LDMS_V_S64:
			fprintf(inst->file, "%"PRId64, val->v_s64);
			break;
		case LDMS_V_S64_ARRAY:
			for (i = 0; i < n; i++) {
				if (i)
					fprintf(inst->file, "%s", ELE_DELIM);
				fprintf(inst->file, "%"PRId64, val->a_s64[i]);
			}
			break;
		case LDMS_V_F32:
			fprintf(inst->file, "%f", val->v_f);
			break;
		case LDMS_V_F32_ARRAY:
			for (i = 0; i < n; i++) {
				if (i)
					fprintf(inst->file, "%s", ELE_DELIM);
				fprintf(inst->file, "%f", val->a_f[i]);
			}
			break;
		case LDMS_V_D64:
			fprintf(inst->file, "%f", val->v_d);
			break;
		case LDMS_V_D64_ARRAY:
			for (i = 0; i < n; i++) {
				if (i)
					fprintf(inst->file, "%s", ELE_DELIM);
				fprintf(inst->file, "%f", val->a_d[i]);
			}
			break;
		default:
			assert(0);
		}
	}
	fprintf(inst->file, "\n");
	pthread_mutex_unlock(&inst->lock);

	return 0;
}

static int flush_store(ldmsd_plug_handle_t handle, ldmsd_store_handle_t sh)
{
	int rc;
	test_store_inst_t inst;
	LIST_FOREACH(inst, &test_store_handle_list, entry) {
		pthread_mutex_lock(&inst->lock);
		rc = fflush(inst->file);
		assert(0 == rc);
		pthread_mutex_unlock(&inst->lock);
	}
	return 0;
}

static struct ldmsd_store test_store = {
	.base = {
		.name = STORE_NAME,
		.type = LDMSD_PLUGIN_STORE,
		.term = term,
		.config = config,
		.usage = usage,
	},
	.open = open_store,
	.close = close_store,
	.store = store,
	.flush = flush_store,
};

struct ldmsd_plugin *get_plugin()
{
	mylog = ovis_log_register("store.test", "Messages for test_store");
	assert(mylog);
	return &test_store.base;
}

static void __attribute__ ((constructor)) test_store_init();
static void test_store_init()
{
	pthread_mutex_init(&cfg_lock, NULL);
	LIST_INIT(&test_store_handle_list);
}

static void __attribute__ ((destructor)) test_store_fini(void);
static void test_store_fini()
{
	pthread_mutex_destroy(&cfg_lock);
}
