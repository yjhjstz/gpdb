%global __os_install_post %{nil}
Name:           postgres-server
Version:    12.1
Release:    1%{?dist}
Summary:    PostgreSQL server with quantum

Group:          ML
License:    PostgreSQL
URL:        http://www.postgresql.org/
Packager:       nickyoung<yangjianghua@didichuxing.com>
Source:         %{name}.tar.gz

BuildRoot: %_topdir/BUILDROOT

%description
PostgreSQL is an advanced Object-Relational database management system (DBMS).
The base postgresql package contains the client programs that you'll need to
access a PostgreSQL DBMS server, as well as HTML documentation for the whole
system.  These client programs can be located on the same machine as the
PostgreSQL server, or on a remote machine that accesses a PostgreSQL server
over a network connection.  The PostgreSQL server can be found in the
postgresql-server sub-package.

%prep
%setup -q


%build
export LIBRARY_PATH=/usr/local/nvidia/lib:/usr/local/nvidia/lib64:/usr/local/cuda/lib64
./configure --with-blocksize=32 --prefix=%_topdir/BUILD/%{name}-%{version}/install
make  %{?_smp_mflags}
make install
cd contrib
make && make install

%install
mkdir -p %{buildroot}/usr/local/pgsql/lib
mkdir -p %{buildroot}/usr/local/pgsql/include
mkdir -p %{buildroot}/usr/local/pgsql/bin
mkdir -p %{buildroot}/usr/local/pgsql/share

cp -rp %_topdir/BUILD/%{name}-%{version}/install/lib/* %{buildroot}/usr/local/pgsql/lib
cp -rp %_topdir/BUILD/%{name}-%{version}/install/include/* %{buildroot}/usr/local/pgsql/include
cp -rp %_topdir/BUILD/%{name}-%{version}/install/bin/* %{buildroot}/usr/local/pgsql/bin
cp -rp %_topdir/BUILD/%{name}-%{version}/install/share/* %{buildroot}/usr/local/pgsql/share

%pre
/usr/sbin/groupadd -g 26 -o -r postgres >/dev/null 2>&1 || :
/usr/sbin/useradd -M -g postgres -o -r -d /var/lib/pgsql -s /bin/bash \
%if 0%{?rhel} >= 6
-N \
%endif
    -c "PostgreSQL Server" -u 26 postgres >/dev/null 2>&1 || :


%clean
rm -rf %{buildroot}

%files
%defattr(-,root,root,-)
/usr/local/pgsql/lib/*
/usr/local/pgsql/include/*
/usr/local/pgsql/bin/*
/usr/local/pgsql/share/*


%doc



%changelog