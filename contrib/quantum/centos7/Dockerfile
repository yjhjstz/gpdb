# "ported" by Adam Miller <maxamillion@fedoraproject.org> from
#   https://github.com/fedora-cloud/Fedora-Dockerfiles
#
# Originally written for Fedora-Dockerfiles by
#   scollier <scollier@redhat.com>

# FROM ml-registry.xiaojukeji.com/centos-luban/cuda10-cudnn7-devel-centos7:20190614
FROM centos:centos7
MAINTAINER The CentOS Project <cloud-ops@centos.org>


RUN yum -y update; yum clean all
RUN yum -y install wget curl sudo epel-release; yum clean all
RUN curl -s https://packagecloud.io/install/repositories/prometheus-rpm/release/script.rpm.sh | sudo bash
RUN yum -y install postgres_exporter supervisor pwgen; yum clean all
RUN yum -y localinstall https://mirrors.cloud.tencent.com/postgresql/repos/yum/12/redhat/rhel-7.7-x86_64/pgpool-II-12-4.1.0-1.rhel7.x86_64.rpm


RUN echo "include=http://didiyum.sys.xiaojukeji.com/didiyum/repo.php" >> /etc/yum.conf
RUN yum -y update; yum  --enablerepo=didi_cloud -y install postgres-server

RUN mkdir -p /etc/postgres_exporter
ADD ./queries.yaml /etc/postgres_exporter/queries.yaml
ADD ./postgres_exporter.sql /etc/postgres_exporter/postgres_exporter.sql
ADD ./postgresql-setup /usr/bin/postgresql-setup
ADD ./supervisord.conf /etc/supervisord.conf
ADD ./start_postgres.sh /start_postgres.sh

#Sudo requires a tty. fix that.
RUN sed -i 's/.*requiretty$/#Defaults requiretty/' /etc/sudoers
RUN chmod +x /usr/bin/postgresql-setup
RUN chmod +x /start_postgres.sh

RUN /usr/bin/postgresql-setup initdb

ADD ./postgresql.conf /var/lib/pgsql/data/postgresql.conf

RUN chown -v postgres.postgres /var/lib/pgsql/data/postgresql.conf

RUN echo "host    all             all             0.0.0.0/0               md5" >> /var/lib/pgsql/data/pg_hba.conf

VOLUME ["/var/lib/pgsql"]

EXPOSE 5432
EXPOSE 9187

CMD ["/bin/bash", "/start_postgres.sh"]
