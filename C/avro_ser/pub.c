#include <stdio.h>
#include <avro.h>
#include <libserdes/serdes.h>

#include "ldms/ldms_stream_avro_ser.h"

int main(int argc, char **argv)
{
	avro_value_t av;
	serdes_conf_t *sd_conf;
	serdes_t *sd;
	serdes_schema_t *sd_sch = NULL;
	avro_schema_t av_sch;
	avro_value_iface_t *av_class;
	ldms_t x;
	int rc;
	char c;
	char ebuf[4096];

	char sch_def[] =
		"{\"type\":\"record\",\
		  \"name\":\"User\",\
		  \"fields\":[\
		      {\"name\": \"name\", \"type\": \"string\"},\
		      {\"name\": \"uid\", \"type\": \"int\"},\
		      {\"name\": \"gid\", \"type\": \"int\"}]}";


	x = ldms_xprt_new_with_auth("sock", "none", NULL);
	assert(x);
	rc = ldms_xprt_connect_by_name(x, "localhost", "411", NULL, NULL);
	assert(rc == 0);
	sd_conf = serdes_conf_new(ebuf, sizeof(ebuf),
			"schema.registry.url", "http://schema-registry-1:8081",
			NULL);
	assert(sd_conf);
	sd = serdes_new(sd_conf, ebuf, sizeof(ebuf));
	assert(sd);
	rc = avro_schema_from_json(sch_def, sizeof(sch_def), &av_sch, NULL);
	assert(rc == 0);
	av_class = avro_generic_class_from_schema(av_sch);
	assert(av_class);
	rc = avro_generic_value_new(av_class, &av);
	assert(rc == 0);

	avro_value_t av_name, av_uid, av_gid;
	avro_value_get_by_name(&av, "name", &av_name, NULL);
	avro_value_set_string(&av_name, "bob");
	avro_value_get_by_name(&av, "uid", &av_uid, NULL);
	avro_value_set_int(&av_uid, 1000);
	avro_value_get_by_name(&av, "gid", &av_gid, NULL);
	avro_value_set_int(&av_gid, 2000);

	char *js = NULL;

	rc = avro_value_to_json(&av, 0, &js);
	assert(rc == 0);
	printf("sending: %s\n", js);

	rc = ldms_stream_publish_avro_ser(x, "avro", NULL, 0400, &av, sd, &sd_sch);
	assert(rc == 0);
	printf("Press ENTER to terminate ...");
	scanf("%c", &c);
	return 0;
}
