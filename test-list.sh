#!/bin/bash
#
# This file defines test lists for various kinds of tests.
# test-all.sh and cygnus-weekly.sh use lists defined in this file.

# List of PAPI-related, containerized tests
PAPI_CONT_TEST_LIST=(
	agg_slurm_test
	papi_sampler_test
	papi_store_test
	store_app_test
	syspapi_test
)

# List of non-PAPI-related, containerized tests
CONT_TEST_LIST=(
	agg_test
	failover_test
	ldmsd_auth_ovis_test
	ldmsd_auth_test
	ldmsd_ctrl_test
	ldmsd_stream_test2
	maestro_cfg_test
	mt-slurm-test
	ovis_ev_test
	prdcr_subscribe_test
	set_array_test
	setgroup_test
	slurm_stream_test
	spank_notifier_test
	ldms_list_test
	quick_set_add_rm_test
	set_array_hang_test
	ldmsd_autointerval_test
	ldms_record_test
	ldms_schema_digest_test
	ldmsd_decomp_test
	ldmsd_decomp_no_fill_test
	ldmsd_stream_status_test
	store_list_record_test
	maestro_raft_test
	ovis_json_test
	updtr_add_test
	updtr_del_test
	updtr_match_add_test
	updtr_match_del_test
	updtr_prdcr_add_test
	updtr_prdcr_del_test
	updtr_start_test
	updtr_status_test
	updtr_stop_test
	ldmsd_flex_decomp_test
	ldms_set_info_test
	slurm_sampler2_test
	libovis_log_test
	ldmsd_long_config_test
	ldms_rail_test
	ldmsd_rail_test
	ldms_stream_test
	set_sec_mod_test
	dump_cfg_test
	ldmsd_stream_rate_test
	ldms_rate_test
	ldms_ipv6_test

	ldmsd_decomp_static_omit_test
	ldmsd_decomp_static_op_test
	ldmsd_decomp_static_rowcache_test

	json_stream_sampler_test

	ldms_qgroup_test
	ldmsd_qgroup_test

	ldmsd_sampler_exclusive_thread_test

	# Peer Daemon Advertisement
	peer_daemon_advertisement_test

	# old test, shall be removed when we move to 4.5 keep here for a
	#   refernece for now
	#
	#ldmsd_stream_test

	# tests related to multi-instance plugins
	#   hidden for now, until the multi-instance
	#   feature is merged into OVIS-4
	#
	#multi_json_stream_sampler_test
	#multi_procnetdev2_test
	#multi_store_avro_kafka_test
	#multi_store_csv_test
	#multi_store_sos_test
	#multi_test_sampler_test
	#cfgobj_ref_test

	# wait for schema registry feature to merge into maestro
	#maestro_schema_registry_test
)

INSIDE_CONT_TEST_LIST=(
	prdcr_config_cmd_test
	strgp_config_cmd_test
	plugin_config_cmd_test
)

# List of direct (non-containerized, running on host) tests
DIRECT_TEST_LIST=(
	direct_ldms_ls_conn_test
	direct_prdcr_subscribe_test
)
