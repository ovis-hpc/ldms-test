#include <stdio.h>
#include <avro.h>
#include <libserdes/serdes.h>

#include <unistd.h>
#include "ldms/ldms.h"

#include "ldms/ldms_msg_avro_ser.h"
int msg_cb(ldms_msg_avro_ser_event_t aev, void *cb_arg)
{
	ldms_msg_event_t ev = &aev->ev;
	printf("msg_event: %s(%d)\n", ldms_msg_event_type_sym(ev->type),
					 ev->type);
	if (ev->type != LDMS_MSG_EVENT_RECV) {
		printf("  -- ignored --\n");
		return 0;
	}
	printf("msg_type: %s(%d)\n", ldms_msg_type_sym(ev->recv.type),
					ev->recv.type);
	if (ev->recv.type != LDMS_MSG_AVRO_SER) {
		printf("  -- ignored --\n");
		return 0;
	}

	int rc;
	avro_value_t *av = aev->avro_value;
	char *js = NULL;
	rc = avro_value_to_json(av, 1, &js);
	if (rc == 0)
		printf("data: %s\n", js);
	free(js);
	return 0;
}

int main(int argc, char **argv)
{
	ldms_t x = ldms_xprt_new_with_auth("sock", "none", NULL);
	char ebuf[4096];
	int rc;
	char c;
	serdes_t *sd;
	serdes_conf_t *sd_conf;
	ldms_msg_client_t cli;
	rc = ldms_xprt_connect_by_name(x, "localhost", "411", NULL, NULL);
	assert(rc == 0);
	sd_conf = serdes_conf_new(ebuf, sizeof(ebuf),
			"schema.registry.url", "http://schema-registry-1:8081",
			NULL);
	assert(sd_conf);
	sd = serdes_new(sd_conf, ebuf, sizeof(ebuf));
	assert(sd);
	cli = ldms_msg_subscribe_avro_ser("avro", 0, msg_cb, NULL,
					     "avro msg client", sd);
	assert(cli);
	rc = ldms_msg_remote_subscribe(x, "avro", 0, NULL, NULL, LDMS_UNLIMITED);
	assert(rc == 0);
	scanf("%c", &c);
	return 0;
}
