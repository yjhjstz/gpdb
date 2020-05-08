#!/bin/bash

DB_NAME=${POSTGRES_DB:-postgres}
DB_USER=${POSTGRES_USER:-$1}
DB_PASS=${POSTGRES_PASSWORD:-$2}
PG_CONFDIR="/var/lib/pgsql/data"
export LD_LIBRARY_PATH=/usr/local/pgsql/lib:$LD_LIBRARY_PATH

__create_user() {
  #Grant rights
  usermod -G wheel postgres

  # Check to see if we have pre-defined credentials to use
if [ -n "${DB_USER}" ]; then
  if [ -z "${DB_PASS}" ]; then
    echo ""
    echo "WARNING: "
    echo "No password specified for \"${DB_USER}\". Generating one"
    echo ""
    DB_PASS=$(pwgen -c -n -1 12)
    echo "Password for \"${DB_USER}\" created as: \"${DB_PASS}\""
  fi
    echo "Creating user \"${DB_USER}\"..."
    echo "CREATE ROLE ${DB_USER} with CREATEROLE login superuser PASSWORD '${DB_PASS}';" |
      sudo -u postgres -H /usr/local/pgsql/bin/postgres --single \
       -c config_file=${PG_CONFDIR}/postgresql.conf -D ${PG_CONFDIR}
  
fi

if [ -n "${DB_NAME}" ]; then
  echo "Creating database \"${DB_NAME}\"..."
  echo "CREATE DATABASE ${DB_NAME};" | \
    sudo -u postgres -H /usr/local/pgsql/bin/postgres --single \
     -c config_file=${PG_CONFDIR}/postgresql.conf -D ${PG_CONFDIR}

  if [ -n "${DB_USER}" ]; then
    echo "Granting access to database \"${DB_NAME}\" for user \"${DB_USER}\"..."
    echo "GRANT ALL PRIVILEGES ON DATABASE ${DB_NAME} to ${DB_USER};" |
      sudo -u postgres -H /usr/local/pgsql/bin/postgres --single \
      -c config_file=${PG_CONFDIR}/postgresql.conf -D ${PG_CONFDIR}
  fi
fi
}

__create_exporter() {
   echo "Creating postgres_exporter..."
   cat /etc/postgres_exporter/postgres_exporter.sql |
     sudo -u postgres -H /usr/local/pgsql/bin/postgres --single \
      -c config_file=${PG_CONFDIR}/postgresql.conf -D ${PG_CONFDIR}
   #sudo -u postgres -H /usr/local/pgsql/bin/psql -U postgres -d postgres -a -f /etc/postgres_exporter/postgres_exporter.sql
}
__run_supervisor() {
supervisord -n
}

# Call all functions
__create_user
#__create_exporter
__run_supervisor

