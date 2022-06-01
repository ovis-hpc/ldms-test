from ovishpc/ovis-centos-build

SHELL [ "/bin/bash", "-c" ]

RUN set -e ;\
    yum install -y java-11-openjdk ;\
    yum clean all ;

RUN set -e ;\
    pushd /opt/ ;\
    curl -LO https://archive.apache.org/dist/kafka/2.1.0/kafka_2.12-2.1.0.tgz ;\
    tar xzf kafka_2.12-2.1.0.tgz ; \
    mkdir kafka_2.12-2.1.0/logs/ ;

RUN set -e ;\
    yum install -y librdkafka-devel ;\
    yum clean all ;
