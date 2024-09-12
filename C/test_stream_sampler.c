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
 * \file job.c
 * \brief shared job data provider
 */
#define _GNU_SOURCE
#include <inttypes.h>
#include <unistd.h>
#include <sys/errno.h>
#include <stdlib.h>
#include <stdio.h>
#include <stdarg.h>
#include <string.h>
#include <fcntl.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <sys/inotify.h>
#include <time.h>
#include <pthread.h>
#include <strings.h>
#include <ctype.h>
#include <pwd.h>
#include <grp.h>
#include <coll/htbl.h>
#include <ovis_json/ovis_json.h>
#include <ovis_log/ovis_log.h>
#include <assert.h>
#include <sched.h>
#include "ldms/ldms.h"
#include "ldms/ldmsd.h"
#include "ldms/ldmsd_stream.h"

#include "ovis-ldms-config.h" /* for OVIS_GIT_LONG */

static ovis_log_t mylog;

static char tada_user[64]; /* populated in get_plugin */

static char *stream;

static const char *usage(struct ldmsd_plugin *self)
{
	return  "config name=test_stream_sampler path=<path> port=<port_no> log=<path>\n"
		"     output    The path to a file to dump stream output to.\n";
}

static int sample(struct ldmsd_sampler *self)
{
	return 0;
}

static int test_stream_recv_cb(ldms_stream_event_t ev, void *cb_arg);

FILE *out = NULL;

static int config(struct ldmsd_plugin *self, struct attr_value_list *kwl, struct attr_value_list *avl)
{
	char *value;
	int rc;

	value = av_value(avl, "stream");
	if (value)
		stream = strdup(value);
	else
		stream = strdup("test_stream");
	ldms_stream_subscribe(stream, 0, test_stream_recv_cb, self, "test_stream_sampler");

	value = av_value(avl, "output");
	if (!value)
		value = "/data/test_stream_sampler.out";
	out = fopen(value, "w");
	if (!out) {
		rc = errno;
		ovis_log(mylog, OVIS_LERROR, "test_stream_sampler: "
			  "cannot open file '%s'\n", value);
	} else {
		rc = 0;
		setbuf(out, NULL); /* no buffer */
	}

	return rc;
}

static int test_stream_recv_cb(ldms_stream_event_t ev, void *cb_arg)
{
	int rc = 0;
	char soh = 1; /* start of heading */
	char stx = 2; /* start of text */
	char etx = 3; /* end of text */
	if (ev->type != LDMS_STREAM_EVENT_RECV)
		return 0;
	fwrite(&soh, 1, 1, out);
	switch (ev->recv.type) {
	case LDMSD_STREAM_STRING:
		fwrite("string", 1, 6, out);
		break;
	case LDMSD_STREAM_JSON:
		fwrite("json", 1, 4, out);
		break;
	default:
		fwrite("unknown", 1, 7, out);
		break;
	}
	fwrite(&stx, 1, 1, out);
	fwrite(ev->recv.data, 1, ev->recv.data_len, out);
	fwrite(&etx, 1, 1, out);
	return rc;
}

static void term(struct ldmsd_plugin *self)
{
	if (out) {
		fclose(out);
		out = NULL;
	}
}

static struct ldmsd_sampler test_stream_sampler = {
	.base = {
		.name = "test_stream_sampler",
		.type = LDMSD_PLUGIN_SAMPLER,
		.term = term,
		.config = config,
		.usage = usage,
	},
	.sample = sample
};

struct ldmsd_plugin *get_plugin()
{
	char *__user = getenv("TADA_USER");
	if (__user)
		snprintf(tada_user, sizeof(tada_user), "%s", __user);
	else
		getlogin_r(tada_user, sizeof(tada_user));
	mylog = ovis_log_register("sampler.test_stream", "Messages for the test_stream_sampler");
	assert(mylog);
	return &test_stream_sampler.base;
}
