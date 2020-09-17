#include <assert.h>
#include <errno.h>
#include <stdlib.h>
#include <sys/queue.h>
#include <unistd.h>

#include "coll/htbl.h"
#include "ovis_json/ovis_json.h"

#include "tada.h"
#include "util.h"

#define FMT "c:f:l:u:"

#define ASSERT_NO_ENTITY_NEW_INT		1
#define ASSERT_NO_ENTITY_NEW_BOOL		2
#define ASSERT_NO_ENTITY_NEW_FLOAT		3
#define ASSERT_NO_ENTITY_NEW_STRING		4
#define ASSERT_NO_ENTITY_NEW_ATTR		5
#define ASSERT_NO_ENTITY_NEW_LIST		6
#define ASSERT_NO_ENTITY_NEW_DICT		7
#define ASSERT_NO_ENTITY_NEW_NULL		8

#define ASSERT_NO_PARSE_BUFFER_INT		9
#define ASSERT_NO_PARSE_BUFFER_BOOL_FALSE	10
#define ASSERT_NO_PARSE_BUFFER_BOOL_TRUE	11
#define ASSERT_NO_PARSE_BUFFER_FLOAT		12
#define ASSERT_NO_PARSE_BUFFER_STRING		13
#define ASSERT_NO_PARSE_BUFFER_LIST		14
#define ASSERT_NO_PARSE_BUFFER_DICT		15
#define ASSERT_NO_PARSE_BUFFER_NULL		16
#define ASSERT_NO_PARSE_INVALID_BUFFER		17

#define ASSERT_NO_ENTITY_DUMP_INT		18
#define ASSERT_NO_ENTITY_DUMP_BOOL_FALSE	19
#define ASSERT_NO_ENTITY_DUMP_BOOL_TRUE		20
#define ASSERT_NO_ENTITY_DUMP_FLOAT		21
#define ASSERT_NO_ENTITY_DUMP_STRING		22
#define ASSERT_NO_ENTITY_DUMP_ATTR		23
#define ASSERT_NO_ENTITY_DUMP_LIST		24
#define ASSERT_NO_ENTITY_DUMP_DICT		25
#define ASSERT_NO_ENTITY_DUMP_NULL		26
#define ASSERT_NO_ENTITY_DUMP_APPEND		27

#define ASSERT_NO_ENTITY_COPY_INT		28
#define ASSERT_NO_ENTITY_COPY_BOOL_FALSE	29
#define ASSERT_NO_ENTITY_COPY_BOOL_TRUE		30
#define ASSERT_NO_ENTITY_COPY_FLOAT		31
#define ASSERT_NO_ENTITY_COPY_STRING		32
#define ASSERT_NO_ENTITY_COPY_ATTR		33
#define ASSERT_NO_ENTITY_COPY_LIST		34
#define ASSERT_NO_ENTITY_COPY_DICT		35
#define ASSERT_NO_ENTITY_COPY_NULL		36

#define ASSERT_NO_ATTR_COUNT			37
#define ASSERT_NO_ATTR_FIND_EXIST		38
#define ASSERT_NO_ATTR_FIND_NOT_EXIST		39
#define ASSERT_NO_VALUE_FIND_EXIST		40
#define ASSERT_NO_VALUE_FIND_NOT_EXIST		41
#define ASSERT_NO_ATTR_ADD_NEW			42
#define ASSERT_NO_ATTR_ADD_REPLACE		43
#define ASSERT_NO_ATTR_REM_EXIST		44
#define ASSERT_NO_ATTR_REM_NOT_EXIST		45

#define ASSERT_NO_DICT_BUILD_CREATE		46
#define ASSERT_NO_DICT_BUILD_ADD_REPLACE_ATTR	47
#define ASSERT_NO_DICT_MERGE			48

#define ASSERT_NO_LIST_LEN			49
#define ASSERT_NO_ITEM_ADD			50
#define ASSERT_NO_ITEM_REM_EXIST		51
#define ASSERT_NO_ITEM_REM_NOT_EXIST		52
#define ASSERT_NO_ITEM_POP_NOT_EXIST		53
#define ASSERT_NO_ITEM_POP_EXIST		54


enum value_e {
	FIRST_VALUE = 0,
	INT_VALUE = FIRST_VALUE,
	BOOL_FALSE_VALUE,
	BOOL_TRUE_VALUE,
	FLOAT_VALUE,
	STRING_VALUE,
	ATTR_VALUE,
	LIST_VALUE,
	DICT_VALUE,
	NULL_VALUE,
	LAST_VALUE = NULL_VALUE,
};

const char *EXP_JSON_STR[] = {
		[INT_VALUE]		= "1",
		[BOOL_FALSE_VALUE]	= "false",
		[BOOL_TRUE_VALUE]	= "true",
		[FLOAT_VALUE]		= "1.100000",
		[STRING_VALUE]		= "\"foo\"",
		[ATTR_VALUE]		= NULL,
		[LIST_VALUE]		= "[1,false,1.100000,\"foo\",[],{},null]",
		[DICT_VALUE]		= "{\"int\":1," \
					   "\"bool\":true," \
					   "\"float\":1.100000," \
					   "\"string\":\"foo\"," \
					   "\"list\":[1,false,1.100000,\"foo\",[],{},null]," \
					   "\"dict\":{\"attr_1\":\"value_1\"}," \
					   "\"null\":null" \
					   "}",
		[NULL_VALUE]		= "null",
	};

struct str_entry {
	char *key;
	char *value;
	LIST_ENTRY(str_entry) entry;
};
LIST_HEAD(str_list, str_entry);

static json_entity_t *CREATE_EXPECTED_ENTITY(enum value_e type)
{
	json_entity_t *exp, v;
	int i, first, last;
	exp = calloc(1, sizeof(*exp) * (LAST_VALUE+1));
	assert(exp);

	if (0 > (int)type) {
		first = FIRST_VALUE;
		last = LAST_VALUE;
	} else {
		first = type;
		last = type;
	}

	for (i = first; i <= last; i++) {
		switch (i) {
		case INT_VALUE:
			exp[i] = json_entity_new(JSON_INT_VALUE, 1);
			break;
		case BOOL_FALSE_VALUE:
			exp[i] = json_entity_new(JSON_BOOL_VALUE, 0);
			break;
		case BOOL_TRUE_VALUE:
			exp[i] = json_entity_new(JSON_BOOL_VALUE, 123);
			break;
		case FLOAT_VALUE:
			exp[i] = json_entity_new(JSON_FLOAT_VALUE, 1.1);
			break;
		case STRING_VALUE:
			exp[i] = json_entity_new(JSON_STRING_VALUE, "foo");
			break;
		case ATTR_VALUE:
			v = json_entity_new(JSON_STRING_VALUE, "foo");
			exp[i] = json_entity_new(JSON_ATTR_VALUE, "name", v);
			break;
		case LIST_VALUE:
			exp[i] = json_entity_new(JSON_LIST_VALUE);
			json_item_add(exp[i], json_entity_new(JSON_INT_VALUE, 1));
			json_item_add(exp[i], json_entity_new(JSON_BOOL_VALUE, 0));
			json_item_add(exp[i], json_entity_new(JSON_FLOAT_VALUE, 1.1));
			json_item_add(exp[i], json_entity_new(JSON_STRING_VALUE, "foo"));
			json_item_add(exp[i], json_entity_new(JSON_LIST_VALUE));
			json_item_add(exp[i], json_entity_new(JSON_DICT_VALUE));
			json_item_add(exp[i], json_entity_new(JSON_NULL_VALUE));
			break;
		case DICT_VALUE:
			exp[i] = json_entity_new(JSON_DICT_VALUE);
			json_attr_add(exp[i], "int", json_entity_new(JSON_INT_VALUE, 1));
			json_attr_add(exp[i], "bool",
					json_entity_new(JSON_BOOL_VALUE, 12));
			json_attr_add(exp[i], "float",
					json_entity_new(JSON_FLOAT_VALUE, 1.1));
			json_attr_add(exp[i], "string",
					json_entity_new(JSON_STRING_VALUE, "foo"));
			/* list */
			json_attr_add(exp[i], "list",
					json_entity_new(JSON_LIST_VALUE));
			json_item_add(json_value_find(exp[i], "list"),
					json_entity_new(JSON_INT_VALUE, 1));
			json_item_add(json_value_find(exp[i], "list"),
					json_entity_new(JSON_BOOL_VALUE, 0));
			json_item_add(json_value_find(exp[i], "list"),
					json_entity_new(JSON_FLOAT_VALUE, 1.1));
			json_item_add(json_value_find(exp[i], "list"),
					json_entity_new(JSON_STRING_VALUE, "foo"));
			json_item_add(json_value_find(exp[i], "list"),
					json_entity_new(JSON_LIST_VALUE));
			json_item_add(json_value_find(exp[i], "list"),
					json_entity_new(JSON_DICT_VALUE));
			json_item_add(json_value_find(exp[i], "list"),
					json_entity_new(JSON_NULL_VALUE));
			/* dict */
			json_attr_add(exp[i], "dict",
					json_entity_new(JSON_DICT_VALUE));
			json_attr_add(json_value_find(exp[i], "dict"), "attr_1",
					json_entity_new(JSON_STRING_VALUE, "value_1"));
			json_attr_add(exp[i], "null",
					json_entity_new(JSON_NULL_VALUE));
			break;
		case NULL_VALUE:
			exp[i] = json_entity_new(JSON_NULL_VALUE);
			break;
		default:
			assert(0);
			break;
		}
	}


	return exp;
}

static void FREE_EXPECTED_ENTITY(json_entity_t *exp)
{
	int type;
	for (type = FIRST_VALUE; type <= LAST_VALUE; type++)
		json_entity_free(exp[type]);
	free(exp);
}

static struct str_entry *str_entry_new(const char *key, const char *value)
{
	struct str_entry *e;
	e = malloc(sizeof(*e));
	assert(e);
	e->key = strdup(key);
	e->value = strdup(value);
	assert(e->key);
	assert(e->value);
	return e;
}

static struct str_list *dict_str2list(const char *str)
{
	int i;
	size_t cnt = strlen(str);
	char *s, *key, *value;
	struct str_entry *entry;
	struct str_list *list = malloc(sizeof(*list));
	assert(list);

	s = strdup(str);
	assert(s);

	LIST_INIT(list);

	s[0] = s[strlen(s) - 1] = ',';

	key = &s[1];
	i = 2;
	while (i < cnt) {
		if (s[i] == ':') {
			/* value */
			s[i] = '\0';
			value = &s[i+1];
		} else if (s[i] == '[') {
			/* A list */
			do {
				i++;
			} while (s[i] != ']');
		} else if (s[i] == '{') {
			/* dict */
			do {
				i++;
			} while (s[i] != '}');
		} else if (s[i] == ',') {
			/* next attribute */
			s[i] = '\0';
			entry = str_entry_new(key, value);
			LIST_INSERT_HEAD(list, entry, entry);
			i++;
			key = &s[i];
		}
		i++;
	}
	free(s);
	return list;
}

static void str_entry_free(struct str_entry *entry)
{
	free(entry->value);
	free(entry->key);
	free(entry);
}

static void str_list_free(struct str_list *list)
{
	struct str_entry *e;
	e = LIST_FIRST(list);
	while (e) {
		LIST_REMOVE(e, entry);
		str_entry_free(e);
		e = LIST_FIRST(list);
	}
	free(list);
}

static int is_same_dict_str(const char *a, const char *b);
static int is_same_str_entry(struct str_entry *a, struct str_entry *b)
{
	if (0 != strcmp(a->key, b->key))
		return 0;
	if ('{' == a->value[0]) {
		/* dict */
		if ('{' != b->value[0])
			return 0;
		return is_same_dict_str(a->value, b->value);
	}
	if (0 != strcmp(a->value, b->value))
		return 0;
	return 1;
}

static int is_same_dict_str(const char *exp_str, const char *dumped_str)
{
	size_t ecnt, dcnt;
	struct str_list *el, *dl;
	struct str_entry *e, *d;

	ecnt = strlen(exp_str);
	dcnt = strlen(dumped_str);
	if (ecnt != dcnt)
		return 0;
	assert(exp_str[0] == '{');
	assert(exp_str[ecnt - 1] == '}');

	if (('{' != dumped_str[0]) || ('}' != dumped_str[dcnt - 1]))
		return 0;

	el = dict_str2list(exp_str);
	dl = dict_str2list(dumped_str);
	assert(el);
	assert(dl);

	d = LIST_FIRST(dl);
	while (d) {
		e = LIST_FIRST(el);
		while (e) {
			if (is_same_str_entry(d, e)) {
				/* found */
				LIST_REMOVE(e, entry);
				str_entry_free(e);
				goto found;
			} else {
				e = LIST_NEXT(e, entry);
			}
		}
		/* not found in el */
		d = LIST_NEXT(d, entry);
		continue;

	found:
		LIST_REMOVE(d, entry);
		str_entry_free(d);
		d = LIST_FIRST(dl);
	}
	if (!LIST_EMPTY(el) || !LIST_EMPTY(dl)) {
		str_list_free(el);
		str_list_free(dl);
		return 0;
	}
	return 1;
}

static int is_same_entity(json_entity_t l, json_entity_t r)
{
	json_entity_t a, b;

	if (l->type != r->type)
		return 0;

	switch (l->type) {
	case JSON_INT_VALUE:
		return (l->value.int_ == r->value.int_)?1:0;
	case JSON_BOOL_VALUE:
		if (((l->value.bool_ == 0) && (r->value.bool_ == 0)) ||
			((l->value.bool_ != 0) && (r->value.bool_ != 0))) {
			return 1;
		} else {
			return 0;
		}
	case JSON_FLOAT_VALUE:
		return (l->value.double_ == r->value.double_)?1:0;
	case JSON_STRING_VALUE:
		return (0 == strcmp(l->value.str_->str, r->value.str_->str))?1:0;
	case JSON_ATTR_VALUE:
		if (0 != strcmp(json_attr_name(l)->str, json_attr_name(r)->str))
			return 0;
		else
			return is_same_entity(json_attr_value(l), json_attr_value(r));
	case JSON_LIST_VALUE:
		if (json_list_len(l) != json_list_len(r))
			return 0;
		/* The order MUST be the same */
		for (a = json_item_first(l), b = json_item_first(r); a && b;
			a = json_item_next(a), b = json_item_next(b)) {
			if (!is_same_entity(a, b))
				return 0;
		}
		break;
	case JSON_DICT_VALUE:
		a = json_attr_first(l);
		while (a) {
			b = json_value_find(r, json_attr_name(a)->str);
			if (!b)
				return 0;
			if (!is_same_entity(json_attr_value(a), b))
				return 0;
			a = json_attr_next(a);
		}
		/* Check the other way around in case that x has more attributes than e */
		b = json_attr_first(r);
		while (b) {
			a = json_value_find(l, json_attr_name(b)->str);
			if (!a)
				return 0;
			/*
			 * No need to check if a == b.
			 * It was verified in the above loop.
			 */
			b = json_attr_next(b);
		}
		break;
	default:
		break;
	}
	return 1;
}

static void test_json_entity_new(test_t suite)
{
	json_entity_t e, attr_value;
	enum json_value_e type;

	for (type = JSON_INT_VALUE; type <= JSON_NULL_VALUE; type++) {
		switch (type) {
		case JSON_INT_VALUE:
			e = json_entity_new(type, 1);
			tada_assert(suite, ASSERT_NO_ENTITY_NEW_INT,
				((type == e->type) && (1 == e->value.int_)),
				"(type is JSON_INT_VALUE) && (1 == e->value.int_)");
			break;
		case JSON_BOOL_VALUE:
			e = json_entity_new(type, 1);
			tada_assert(suite, ASSERT_NO_ENTITY_NEW_BOOL,
				((type == e->type) && (1 == e->value.bool_)),
				"(type is JSON_BOOL_VALUE) && (1 == e->value.bool_)");
			break;
		case JSON_FLOAT_VALUE:
			e = json_entity_new(type, 1.1);
			tada_assert(suite, ASSERT_NO_ENTITY_NEW_FLOAT,
				((type == e->type) && (1.1 == e->value.double_)),
				"(type is JSON_FLOAT_VALUE) && (1.1 == e->value.double_)");
			break;
		case JSON_STRING_VALUE:
			e = json_entity_new(type, "foo");
			tada_assert(suite, ASSERT_NO_ENTITY_NEW_STRING,
				((type == e->type) &&
					(0 == strcmp("foo", e->value.str_->str))),
				"(type is JSON_STRING_VALUE) && (foo == e->value.str_->str)");
			break;
		case JSON_ATTR_VALUE:
			attr_value = json_entity_new(JSON_STRING_VALUE, "value");
			e = json_entity_new(type, "name", attr_value);
			tada_assert(suite, ASSERT_NO_ENTITY_NEW_ATTR,
				((type == e->type) &&
					(0 == strcmp("name", json_attr_name(e)->str)) &&
					0 == strcmp("value", json_value_str(json_attr_value(e))->str)),
				"(type is JSON_ATTR_VALUE) && " \
					"(name == <attr name>) && " \
					"(value == <attr value>)");
			break;
		case JSON_LIST_VALUE:
			e = json_entity_new(type);
			tada_assert(suite, ASSERT_NO_ENTITY_NEW_LIST,
				((type == e->type) &&
					(0 == e->value.list_->item_count) &&
					(TAILQ_EMPTY(&e->value.list_->item_list))),
				"(type is JSON_LIST_VALUE) && " \
					"(0 == Number of elements) && " \
					"(list is empty)");
			break;
		case JSON_DICT_VALUE:
			e = json_entity_new(type);
			tada_assert(suite, ASSERT_NO_ENTITY_NEW_DICT,
				((type == e->type) && htbl_empty(e->value.dict_->attr_table)),
				"(type is JSON_DICT_VALUE) && (dict table is empty)");
			break;
		case JSON_NULL_VALUE:
			e = json_entity_new(JSON_NULL_VALUE);
			tada_assert(suite, ASSERT_NO_ENTITY_NEW_NULL,
				((type == e->type) && (0 == e->value.int_)),
				"(type is JSON_NULL_VALUE) && (0 == e->value.int_)");
			break;
		default:
			assert(0 == "unrecognized type");
			break;
		}
		json_entity_free(e);
	}
}

static void test_json_entity_dump(test_t suite)
{
	json_entity_t e, e2, a, b;
	jbuf_t jb, jb2;
	jb = jb2 = NULL;
	enum json_value_e type;
	ldms_test_buf_t buf;
	json_entity_t *exp;
	int assert_no;

	exp = CREATE_EXPECTED_ENTITY(-1);
	assert(exp);

	e = e2 = a = b = NULL;
	buf = ldms_test_buf_alloc(512);

	for (type = FIRST_VALUE; type <= LAST_VALUE; type++) {
		assert_no = ASSERT_NO_ENTITY_DUMP_INT + type;
		jb = json_entity_dump(NULL, exp[type]);

		if (type == ATTR_VALUE) {
			tada_assert(suite, assert_no, (NULL == jb), "NULL == jb");
		} else if (type == DICT_VALUE) {
			ldms_test_buf_append(buf, "%s == %s",
					EXP_JSON_STR[type], jb->buf);
			tada_assert(suite, assert_no,
				is_same_dict_str(EXP_JSON_STR[type], jb->buf),
				buf->buf);
		} else {
			ldms_test_buf_append(buf, "%s == %s",
					EXP_JSON_STR[type], jb->buf);
			tada_assert(suite, assert_no,
				(0 == strcmp(EXP_JSON_STR[type], jb->buf)),
				buf->buf);
		}
		ldms_test_buf_reset(buf);
		if (jb)
			jbuf_free(jb);
	}
	FREE_EXPECTED_ENTITY(exp);

	/* dump and append to an existing buffer */
	jb = jbuf_new();
	assert(jb);
	jbuf_append_str(jb, "This is a book.");
	e = json_entity_new(JSON_STRING_VALUE, "FOO");
	assert(e);
	jb =json_entity_dump(jb, e);
	ldms_test_buf_append(buf, "This is a book.\"FOO\" == %s", jb->buf);
	tada_assert(suite, ASSERT_NO_ENTITY_DUMP_APPEND,
			0 == strcmp("This is a book.\"FOO\"", jb->buf), buf->buf);
	jbuf_free(jb);
	json_entity_free(e);

	ldms_test_buf_free(buf);
}

static void test_json_parse_buffer(test_t suite)
{
	json_entity_t d, o, *exp;
	json_parser_t p;
	int rc, type, assert_no;
	int cnt = ASSERT_NO_PARSE_BUFFER_NULL - ASSERT_NO_PARSE_BUFFER_INT + 1;

	char *txt[] = {
		[INT_VALUE]		= "1",
		[BOOL_FALSE_VALUE]	= "false",
		[BOOL_TRUE_VALUE]	= "true",
		[FLOAT_VALUE]		= "1.100000",
		[STRING_VALUE]		= "\"foo\"",
		[ATTR_VALUE]		= NULL,
		[LIST_VALUE]		= "[1,false,	1.100000,   \"foo\",[],\n{},null]",
		[DICT_VALUE]		= "{\"int\":1,\n" \
					   "\"bool\": true," \
					   "\"float\" : 1.1," \
					   "\"string\"	:\"foo\"," \
					   "\"list\":[1,false, 1.1,	\"foo\",   [], {},null]," \
					   "\"dict\"	:	{\"attr_1\":\"value_1\"}," \
					   "\"null\":	null" \
					   "}",
		[NULL_VALUE]		= "null",
	};

	exp = CREATE_EXPECTED_ENTITY(-1);
	assert(exp);

	for (type = FIRST_VALUE; type <= LAST_VALUE; type++) {
		if (type == ATTR_VALUE)
			continue;
		assert_no = ASSERT_NO_PARSE_BUFFER_INT + type;
		p = json_parser_new(0);
		assert(p);
		rc = json_parse_buffer(p, txt[type], strlen(txt[type]), &o);
		tada_assert(suite, assert_no,
				(0 == rc) && is_same_entity(exp[type], o),
				"(0 == json_parse_buffer()) && "
				"is_same_entity(expected, o)");

		json_parser_free(p);
		if (o) {
			json_entity_free(o);
			o = NULL;
		}

	}
	FREE_EXPECTED_ENTITY(exp);

	/* invalid string */
	char *inval_str = "{name:\"book\"}";
	o = NULL;
	p = json_parser_new(0);
	assert(p);
	rc = json_parse_buffer(p, inval_str, strlen(inval_str), &o);
	tada_assert(suite, ASSERT_NO_PARSE_INVALID_BUFFER,
			(0 != rc), "0 != json_parse_buffer()");
	json_parser_free(p);
	if (o)
		json_entity_free(o);
}

static void test_json_entity_copy(test_t suite)
{
	json_entity_t *exp, c;
	int type, assert_no;
	exp = CREATE_EXPECTED_ENTITY(-1);
	for (type = FIRST_VALUE; type <= LAST_VALUE; type++) {
		assert_no = ASSERT_NO_ENTITY_COPY_INT + type;
		c = json_entity_copy(exp[type]);
		assert(c);
		tada_assert(suite, assert_no,
				is_same_entity(exp[type], c),
				"is_same_entity(expected, json_entity_copy(expected)");
		json_entity_free(c);
	}
	FREE_EXPECTED_ENTITY(exp);
}

static void test_dict_apis(test_t suite)
{
	json_entity_t *exp, e, a;
	int rc;

	exp = CREATE_EXPECTED_ENTITY(DICT_VALUE);
	assert(exp);

	/* json_attr_count */
	tada_assert(suite, ASSERT_NO_ATTR_COUNT,
			7 == json_attr_count(exp[DICT_VALUE]),
			"7 == json_attr_count(dict)");

	/* json_attr_find existing attr */
	a = json_attr_find(exp[DICT_VALUE], "list");
	tada_assert(suite, ASSERT_NO_ATTR_FIND_EXIST, (0 != a),
			"0 != json_attr_find()");

	/* json_attr_find not-existing attr */
	a = json_attr_find(exp[DICT_VALUE], "none");
	tada_assert(suite, ASSERT_NO_ATTR_FIND_NOT_EXIST, (0 == a),
			"0 == json_attr_find()");

	/* json_value_find existing attr */
	a = json_value_find(exp[DICT_VALUE], "float");
	tada_assert(suite, ASSERT_NO_VALUE_FIND_EXIST, (0 != a),
			"0 != json_value_find()");

	/* json_value_find not-existing attr */
	a = json_value_find(exp[DICT_VALUE], "none");
	tada_assert(suite, ASSERT_NO_VALUE_FIND_NOT_EXIST, (0 == a),
			"0 == json_value_find()");

	/* json_attr_add a new attribute */
	e = json_entity_new(JSON_STRING_VALUE, "new");
	assert(e);
	rc = json_attr_add(exp[DICT_VALUE], "just_added", e);
	a = json_attr_find(exp[DICT_VALUE], "just_added");
	tada_assert(suite, ASSERT_NO_ATTR_ADD_NEW,
			(0 == rc) && a,
			"(0 == json_attr_add() && (0 != json_attr_find())");

	/* json_attr_add replaces the value */
	e = json_entity_new(JSON_STRING_VALUE, "replace");
	assert(e);
	rc = json_attr_add(exp[DICT_VALUE], "just_added", e);
	a = json_value_find(exp[DICT_VALUE], "just_added");
	tada_assert(suite, ASSERT_NO_ATTR_ADD_REPLACE,
			(0 == rc) && a && (is_same_entity(e, a)),
			"(0 == json_attr_add()) && "
			"(0 != json_value_find()) && "
			"(is_same_entity(old_v, new_v))");

	/* json_attr_rem existing attr */
	rc = json_attr_rem(exp[DICT_VALUE], "just_added");
	a = json_attr_find(exp[DICT_VALUE], "just_added");
	tada_assert(suite, ASSERT_NO_ATTR_REM_EXIST,
			(0 == rc) && !a,
			"(0 = json_attr_rem()) && (0 == json_attr_find())");

	/* json_attr_rem non-existing attr */
	rc = json_attr_rem(exp[DICT_VALUE], "none");
	tada_assert(suite, ASSERT_NO_ATTR_REM_NOT_EXIST,
			(ENOENT == rc), "(ENOENT == json_attr_rem())");
	FREE_EXPECTED_ENTITY(exp);
}

static void test_dict_build(test_t suite)
{
	json_entity_t d, *exp, a, b;

	exp = CREATE_EXPECTED_ENTITY(DICT_VALUE);
	assert(exp);
	json_attr_add(exp[DICT_VALUE], "attr",
			json_entity_new(JSON_STRING_VALUE, "value"));

	a = json_entity_new(JSON_ATTR_VALUE, "attr",
			json_entity_new(JSON_STRING_VALUE, "value"));
	assert(a);

	d = json_dict_build(NULL,
			JSON_INT_VALUE, "int", 1,
			JSON_BOOL_VALUE, "bool", 1,
			JSON_FLOAT_VALUE, "float", 1.1,
			JSON_STRING_VALUE, "string", "foo",
			JSON_LIST_VALUE, "list",
				JSON_INT_VALUE, 1,
				JSON_BOOL_VALUE, 0,
				JSON_FLOAT_VALUE, 1.1,
				JSON_STRING_VALUE, "foo",
				JSON_LIST_VALUE, -2,
				JSON_DICT_VALUE, -2,
				JSON_NULL_VALUE,
				-2,
			JSON_DICT_VALUE, "dict",
				JSON_STRING_VALUE, "attr_1", "value_1",
				-2,
			JSON_NULL_VALUE, "null",
			JSON_ATTR_VALUE, a,
			-1);
	assert(d);

	tada_assert(suite, ASSERT_NO_DICT_BUILD_CREATE,
			is_same_entity(exp[DICT_VALUE], d),
			"expected == json_dict_build(...)");

	/* add & replace more attributes */
	json_attr_add(exp[DICT_VALUE], "int", json_entity_new(JSON_INT_VALUE, 2));
	a = json_entity_new(JSON_DICT_VALUE);
	json_attr_add(a, "a", json_entity_new(JSON_STRING_VALUE, "a"));
	json_attr_add(a, "b", json_entity_new(JSON_STRING_VALUE, "b"));
	json_attr_add(exp[DICT_VALUE], "new_1", a);

	d = json_dict_build(d,
			JSON_INT_VALUE, "int", 2,
			JSON_DICT_VALUE, "new_1",
				JSON_STRING_VALUE, "a", "a",
				JSON_STRING_VALUE, "b", "b",
				-2,
			-1);
	tada_assert(suite, ASSERT_NO_DICT_BUILD_ADD_REPLACE_ATTR,
			is_same_entity(exp[DICT_VALUE], d),
			"expected == json_dict_build(d, ...)");
	FREE_EXPECTED_ENTITY(exp);
}

static void test_dict_merge(test_t suite)
{
	int rc;
	json_entity_t *exp, d1, d2, a;

	exp = CREATE_EXPECTED_ENTITY(DICT_VALUE);
	assert(exp);

	d2 = json_dict_build(NULL,
			/* replace */
			JSON_INT_VALUE, "int", 2,
			JSON_NULL_VALUE, "bool",
			JSON_FLOAT_VALUE, "float", 2.2,
			JSON_STRING_VALUE, "string", "bar",
			JSON_LIST_VALUE, "list",
				JSON_INT_VALUE, 1,
				JSON_STRING_VALUE, "haha",
				-2,
			JSON_DICT_VALUE, "dict",
				JSON_STRING_VALUE, "name", "value",
				-2,

			/* new */
			JSON_INT_VALUE, "int_1", 3,
			JSON_BOOL_VALUE, "bool_1", 0,
			JSON_FLOAT_VALUE, "float_1", 3.3,
			JSON_STRING_VALUE, "string_1", "333",
			JSON_LIST_VALUE, "list_1", -2,
			JSON_DICT_VALUE, "dict_1", -2,
			JSON_NULL_VALUE, "null_1",
			-1);
	assert(d2);

	for (a = json_attr_first(d2); a; a = json_attr_next(a)) {
		rc = json_attr_add(exp[DICT_VALUE],
				json_attr_name(a)->str,
				json_attr_value(a));
		assert(0 == rc);
	}

	d1 = json_entity_copy(exp[DICT_VALUE]);
	assert(d1);

	rc = json_dict_merge(d1, d2);
	tada_assert(suite, ASSERT_NO_DICT_MERGE,
			is_same_entity(exp[DICT_VALUE], d1),
			"The merged dictionary is correct.");

	json_entity_free(d1);
	json_entity_free(d2);
	FREE_EXPECTED_ENTITY(exp);
}

static void test_list_apis(test_t suite)
{
	json_entity_t *exp, item1, item2, item3, item;
	int len, rc;
	size_t cnt;
	char exp_str[1024];
	jbuf_t jb;

	exp = CREATE_EXPECTED_ENTITY(LIST_VALUE);
	assert(exp);

	/* json_list_len */
	tada_assert(suite, ASSERT_NO_LIST_LEN,
			7 == json_list_len(exp[LIST_VALUE]),
			"7 == json_list_len()");

	/* json_item_add */
	cnt = snprintf(exp_str, 1024, "%s", EXP_JSON_STR[LIST_VALUE]);
	exp_str[cnt - 1] = '\0';
	snprintf(&exp_str[cnt - 1], 1024 - cnt, ",\"new\",\"foo\"]");

	item1= json_entity_new(JSON_STRING_VALUE, "new");
	assert(item1);
	json_item_add(exp[LIST_VALUE], item1);
	item2 = json_entity_new(JSON_STRING_VALUE, "foo");
	assert(item2);
	item3 = json_entity_new(JSON_STRING_VALUE, "none");
	assert(item3);
	json_item_add(exp[LIST_VALUE], item2);
	jb = json_entity_dump(NULL, exp[LIST_VALUE]);
	assert(jb);
	tada_assert(suite, ASSERT_NO_ITEM_ADD,
			(0 == strcmp(exp_str, jb->buf)),
			"0 == strcmp(exp_str, json_entity_dump(l)->buf");

	/* json_item_rem exist */
	rc = json_item_rem(exp[LIST_VALUE], item1);
	tada_assert(suite, ASSERT_NO_ITEM_REM_EXIST,
			(0 == rc), "0 == json_item_rem()");

	/* json_item_rem not exist */
	rc = json_item_rem(exp[LIST_VALUE], item3);
	tada_assert(suite, ASSERT_NO_ITEM_REM_NOT_EXIST,
			(ENOENT == rc), "ENOENT == json_item_rem()");

	/* json_item_pop idx out-of-range */
	len = json_list_len(exp[LIST_VALUE]);
	item = json_item_pop(exp[LIST_VALUE], len + 3);
	tada_assert(suite, ASSERT_NO_ITEM_POP_NOT_EXIST,
			(NULL == item), "NULL == json_item_pop(len + 3)");

	/* json_item_pop idx in-range */
	item = json_item_pop(exp[LIST_VALUE], len - 1);
	tada_assert(suite, ASSERT_NO_ITEM_POP_EXIST,
			(NULL != item), "NULL != json_item_pop(len - 1)");

	FREE_EXPECTED_ENTITY(exp);
}

int main(int argc, char **argv) {
	int op;
	int test_flags = TADA_TEST_F_LOG_RESULT;
	char *log_path = NULL;
	char *user = "";
	char *commit_id = "";

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

	TEST_BEGIN("OVIS-LIB", "ovis_json_test", "FVT", user,
		   commit_id, "Test the ovis_json library", log_path,
		   test_flags, suite)
	/* json_entity_new */
	TEST_ASSERTION(suite, ASSERT_NO_ENTITY_NEW_INT,
			"Test creating a JSON integer entity")
	TEST_ASSERTION(suite, ASSERT_NO_ENTITY_NEW_BOOL,
			"Test creating a JSON boolean entity")
	TEST_ASSERTION(suite, ASSERT_NO_ENTITY_NEW_FLOAT,
			"Test creating a JSON float entity")
	TEST_ASSERTION(suite, ASSERT_NO_ENTITY_NEW_STRING,
			"Test creating a JSON string entity")
	TEST_ASSERTION(suite, ASSERT_NO_ENTITY_NEW_ATTR,
			"Test creating a JSON attribute entity")
	TEST_ASSERTION(suite, ASSERT_NO_ENTITY_NEW_LIST,
			"Test creating a JSON list entity")
	TEST_ASSERTION(suite, ASSERT_NO_ENTITY_NEW_DICT,
			"Test creating a JSON dictionary entity")
	TEST_ASSERTION(suite, ASSERT_NO_ENTITY_NEW_NULL,
			"Test creating a JSON null entity")

	TEST_ASSERTION(suite, ASSERT_NO_PARSE_BUFFER_INT,
			"Test parsing a JSON integer string")
	TEST_ASSERTION(suite, ASSERT_NO_PARSE_BUFFER_BOOL_FALSE,
			"Test parsing a JSON false boolean string")
	TEST_ASSERTION(suite, ASSERT_NO_PARSE_BUFFER_BOOL_TRUE,
			"Test parsing a JSON true boolean string")
	TEST_ASSERTION(suite, ASSERT_NO_PARSE_BUFFER_FLOAT,
			"Test parsing a JSON float string")
	TEST_ASSERTION(suite, ASSERT_NO_PARSE_BUFFER_STRING,
			"Test parsing a JSON string")
	TEST_ASSERTION(suite, ASSERT_NO_PARSE_BUFFER_LIST,
			"Test parsing a JSON list string")
	TEST_ASSERTION(suite, ASSERT_NO_PARSE_BUFFER_DICT,
			"Test parsing a JSON dict string")
	TEST_ASSERTION(suite, ASSERT_NO_PARSE_BUFFER_NULL,
			"Test parsing a JSON null string")
	TEST_ASSERTION(suite, ASSERT_NO_PARSE_INVALID_BUFFER,
			"Test parsing an invalid string")

	/* json_entity_dump */
	TEST_ASSERTION(suite, ASSERT_NO_ENTITY_DUMP_INT,
			"Test dumping a JSON integer entity")
	TEST_ASSERTION(suite, ASSERT_NO_ENTITY_DUMP_BOOL_FALSE,
			"Test dumping a JSON false boolean entity")
	TEST_ASSERTION(suite, ASSERT_NO_ENTITY_DUMP_BOOL_TRUE,
			"Test dumping a JSON true boolean entity")
	TEST_ASSERTION(suite, ASSERT_NO_ENTITY_DUMP_FLOAT,
			"Test dumping a JSON float entity")
	TEST_ASSERTION(suite, ASSERT_NO_ENTITY_DUMP_STRING,
			"Test dumping a JSON string entity")
	TEST_ASSERTION(suite, ASSERT_NO_ENTITY_DUMP_ATTR,
			"Test dumping a JSON attr entity")
	TEST_ASSERTION(suite, ASSERT_NO_ENTITY_DUMP_LIST,
			"Test dumping a JSON list entity")
	TEST_ASSERTION(suite, ASSERT_NO_ENTITY_DUMP_DICT,
			"Test dumping a JSON dict entity")
	TEST_ASSERTION(suite, ASSERT_NO_ENTITY_DUMP_NULL,
			"Test dumping a JSON null entity")
	TEST_ASSERTION(suite, ASSERT_NO_ENTITY_DUMP_APPEND,
			"Test dumping a JSON entity to a non-empty jbuf")

	/* json_entity_copy */
	TEST_ASSERTION(suite, ASSERT_NO_ENTITY_COPY_INT,
			"Test copying a JSON integer entity")
	TEST_ASSERTION(suite, ASSERT_NO_ENTITY_COPY_BOOL_FALSE,
			"Test copying a JSON false boolean entity")
	TEST_ASSERTION(suite, ASSERT_NO_ENTITY_COPY_BOOL_TRUE,
			"Test copying a JSON true boolean entity")
	TEST_ASSERTION(suite, ASSERT_NO_ENTITY_COPY_FLOAT,
			"Test copying a JSON float entity")
	TEST_ASSERTION(suite, ASSERT_NO_ENTITY_COPY_STRING,
			"Test copying a JSON string entity")
	TEST_ASSERTION(suite, ASSERT_NO_ENTITY_COPY_ATTR,
			"Test copying a JSON attribute entity")
	TEST_ASSERTION(suite, ASSERT_NO_ENTITY_COPY_LIST,
			"Test copying a JSON list entity")
	TEST_ASSERTION(suite, ASSERT_NO_ENTITY_COPY_DICT,
			"Test copying a JSON dict entity")
	TEST_ASSERTION(suite, ASSERT_NO_ENTITY_COPY_NULL,
			"Test copying a JSON null entity")

	/* json dict apis */
	TEST_ASSERTION(suite, ASSERT_NO_ATTR_COUNT,
			"Test obtaining the number of attributes")
	TEST_ASSERTION(suite, ASSERT_NO_ATTR_FIND_EXIST,
			"Test finding an existing attribute")
	TEST_ASSERTION(suite, ASSERT_NO_ATTR_FIND_NOT_EXIST,
			"Test finding a non-existng attribute")
	TEST_ASSERTION(suite, ASSERT_NO_VALUE_FIND_EXIST,
			"Test finding the value of an existing attribute")
	TEST_ASSERTION(suite, ASSERT_NO_VALUE_FIND_NOT_EXIST,
			"Test finding the value of a non-existing attribute")
	TEST_ASSERTION(suite, ASSERT_NO_ATTR_ADD_NEW,
			"Test adding a new attribute to a dictionary")
	TEST_ASSERTION(suite, ASSERT_NO_ATTR_ADD_REPLACE,
			"Test replacing the value of an existing attribute")
	TEST_ASSERTION(suite, ASSERT_NO_ATTR_REM_EXIST,
			"Test removing an existing attribute")
	TEST_ASSERTION(suite, ASSERT_NO_ATTR_REM_NOT_EXIST,
			"Test removing a non-existing attribute")

	TEST_ASSERTION(suite, ASSERT_NO_DICT_BUILD_CREATE,
			"Test creating a dictionary by json_dict_build")
	TEST_ASSERTION(suite, ASSERT_NO_DICT_BUILD_ADD_REPLACE_ATTR,
			"Test adding attributes and replacing attribute values by json_dict_build")

	TEST_ASSERTION(suite, ASSERT_NO_DICT_MERGE,
			"Test json_dict_merge()")

	TEST_ASSERTION(suite, ASSERT_NO_LIST_LEN,
			"Test json_list_len()")
	TEST_ASSERTION(suite, ASSERT_NO_ITEM_ADD,
			"Test adding items to a list")
	TEST_ASSERTION(suite, ASSERT_NO_ITEM_REM_EXIST,
			"Test removing an existing item by json_item_rem()")
	TEST_ASSERTION(suite, ASSERT_NO_ITEM_REM_NOT_EXIST,
			"Test removing a non-existing item by json_item_rem()")
	TEST_ASSERTION(suite, ASSERT_NO_ITEM_POP_NOT_EXIST,
			"Test popping an existing item from a list by json_item_pop()")
	TEST_ASSERTION(suite, ASSERT_NO_ITEM_POP_EXIST,
			"Test popping a non-existing item from a list by json_item_pop()")

	TEST_END(suite);

	TEST_START(suite);
	test_json_entity_new(&suite);
	test_json_parse_buffer(&suite);
	test_json_entity_dump(&suite);
	test_json_entity_copy(&suite);
	test_dict_apis(&suite);
	test_dict_build(&suite);
	test_dict_merge(&suite);
	test_list_apis(&suite);
	TEST_FINISH(suite);
}
