/* -*- c-basic-offset: 8 -*-
 * Copyright (c) 2019 National Technology & Engineering Solutions
 * of Sandia, LLC (NTESS). Under the terms of Contract DE-NA0003525 with
 * NTESS, the U.S. Government retains certain rights in this software.
 * Copyright (c) 2019 Open Grid Computing, Inc. All rights reserved.
 *
 * This software is available to you under a choice of one of two
 * licenses.  You may choose to be licensed under the terms of the GNU
 * General Public License (GPL) Version 2, available from the file
 * COPYING in the main directory of this source tree, or the BSD-type
 * license below:
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions
 * are met:
 *
 *      Redistributions of source code must retain the above copyright
 *      notice, this list of conditions and the following disclaimer.
 *
 *      Redistributions in binary form must reproduce the above
 *      copyright notice, this list of conditions and the following
 *      disclaimer in the documentation and/or other materials provided
 *      with the distribution.
 *
 *      Neither the name of Sandia nor the names of any contributors may
 *      be used to endorse or promote products derived from this software
 *      without specific prior written permission.
 *
 *      Neither the name of Open Grid Computing nor the names of any
 *      contributors may be used to endorse or promote products derived
 *      from this software without specific prior written permission.
 *
 *      Modified source versions must be plainly marked as such, and
 *      must not be misrepresented as being the original software.
 *
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS
 * "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT
 * LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR
 * A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT
 * OWNER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL,
 * SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT
 * LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE,
 * DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY
 * THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
 * (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
 * OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 */

/**
 * \file test_stream_sampler5.c
 *
 * test_stream_sampler for LDMSD v5.
 *
 * The sampler registers for the given ldmsd stream and dumps the stream
 * contents to the given output file.
 *
 * This sampler does NOT create LDMS sets. It also does NOT need `smplr` to
 * periodically update.
 */

#include <stdio.h>

#include "ldms/ldmsd.h"
#include "ldms/ldmsd_sampler.h"
#include "ldms/ldmsd_stream.h"

#define INST(x) ((ldmsd_plugin_inst_t)(x))
#define INST_LOG(inst, lvl, fmt, ...) \
		ldmsd_log((lvl), "%s: " fmt, INST(inst)->inst_name, \
								##__VA_ARGS__)

typedef struct tss_inst_s *tss_inst_t;
struct tss_inst_s {
	struct ldmsd_plugin_inst_s base;
	/* Extend plugin-specific data here */
	char tada_user[64];
	char *stream;
	FILE *out;
	ldmsd_stream_client_t stream_client;
};

/* ============== Sampler Plugin APIs ================= */

static
int tss_update_schema(ldmsd_plugin_inst_t pi, ldms_schema_t schema)
{
	/* do nothing */
	return 0;
}

static
int tss_update_set(ldmsd_plugin_inst_t pi, ldms_set_t set, void *ctxt)
{
	/* do nothing */
	return 0;
}


/* ============== Common Plugin APIs ================= */

static
const char *tss_desc(ldmsd_plugin_inst_t pi)
{
	return "test_stream_sampler - sampler plugin for testing ldmsd stream";
}

static
char *_help = "\
test_stream_sampler configuration synopsis:\n\
    config name=INST stream=STREAM_NAME output=OUTPUT_PATH\n\
\n\
Option descriptions:\n\
    stream The name of the ldmsd stream to register to. (default: test_stream)\n\
    output The path of output file for dumping stream contents.\n\
           (default: /data/test_stream_sampler.out)\n\
";

static
const char *tss_help(ldmsd_plugin_inst_t pi)
{
	return _help;
}

static
int test_stream_recv_cb(ldmsd_stream_client_t c, void *ctxt,
			ldmsd_stream_type_t stream_type,
			const char *msg, size_t msg_len,
			json_entity_t entity)
{
	tss_inst_t inst = ctxt;
	char soh = 1; /* start of heading */
	char stx = 2; /* start of text */
	char etx = 3; /* end of text */
	fwrite(&soh, 1, 1, inst->out);
	switch (stream_type) {
	case LDMSD_STREAM_STRING:
		fwrite("string", 1, 6, inst->out);
		break;
	case LDMSD_STREAM_JSON:
		fwrite("json", 1, 4, inst->out);
		break;
	default:
		fwrite("unknown", 1, 7, inst->out);
		break;
	}
	fwrite(&stx, 1, 1, inst->out);
	fwrite(msg, 1, msg_len, inst->out);
	fwrite(&etx, 1, 1, inst->out);
	return 0;
}

const char *
json_attr_find_str(json_entity_t json, const char *key)
{
	json_entity_t val = json_value_find(json, (void*)key);
	if (!val || val->type != JSON_STRING_VALUE)
		return NULL;
	return json_value_str(val)->str;
}

static
int tss_config(ldmsd_plugin_inst_t pi, json_entity_t json,
				      char *ebuf, int ebufsz)
{
	tss_inst_t inst = (void*)pi;
	ldmsd_sampler_type_t samp = (void*)inst->base.base;
	int rc;
	const char *value;

	rc = samp->base.config(pi, json, ebuf, ebufsz);
	if (rc)
		return rc;


	value = json_attr_find_str(json, "stream");
	if (value)
		inst->stream = strdup(value);
	else
		inst->stream = strdup("test_stream");
	value = json_attr_find_str(json, "output");
	if (!value)
		value = "/data/test_stream_sampler.out";
	inst->out = fopen(value, "w");
	if (!inst->out) {
		rc = errno;
		ldmsd_log(LDMSD_LERROR, "test_stream_sampler: "
			  "cannot open file '%s'\n", value);
	} else {
		rc = 0;
		setbuf(inst->out, NULL); /* no buffer */
	}

	inst->stream_client = ldmsd_stream_subscribe(inst->stream,
						     test_stream_recv_cb, inst);

	if (!inst->stream_client)
		rc = errno;

	return rc;
}

static
void tss_del(ldmsd_plugin_inst_t pi)
{
	tss_inst_t inst = (void*)pi;

	if (inst->stream_client)
		ldmsd_stream_close(inst->stream_client);

	if (inst->out)
		fclose(inst->out);

	if (inst->stream)
		free(inst->stream);
}

static
int tss_init(ldmsd_plugin_inst_t pi)
{
	tss_inst_t inst = (void*)pi;
	ldmsd_sampler_type_t samp = (void*)inst->base.base;
	/* override update_schema() and update_set() */
	samp->update_schema = tss_update_schema;
	samp->update_set = tss_update_set;

	char *__user = getenv("TADA_USER");
	if (__user)
		snprintf(inst->tada_user, sizeof(inst->tada_user), "%s", __user);
	else
		getlogin_r(inst->tada_user, sizeof(inst->tada_user));
	return 0;
}

static
struct tss_inst_s __inst = {
	.base = {
		.version     = LDMSD_PLUGIN_VERSION_INITIALIZER,
		.type_name   = LDMSD_SAMPLER_TYPENAME,
		.plugin_name = "test_stream_sampler",

                /* Common Plugin APIs */
		.desc   = tss_desc,
		.help   = tss_help,
		.init   = tss_init,
		.del    = tss_del,
		.config = tss_config,

	},
	/* plugin-specific data initialization (for new()) here */
};

ldmsd_plugin_inst_t new()
{
	tss_inst_t inst = malloc(sizeof(*inst));
	if (inst)
		*inst = __inst;
	return &inst->base;
}
