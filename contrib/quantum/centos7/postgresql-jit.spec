%define _pgmajor 12
%global __os_install_post %{nil}
Name:           postgres-server
Version:    12.1
Release:    2%{?dist}
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
./configure --with-blocksize=32 --prefix=%_topdir/BUILD/%{name}-%{version}/install --with-llvm LLVM_CONFIG=/opt/rh/llvm-toolset-7/root/usr/bin/llvm-config CLANG=/opt/rh/llvm-toolset-7/root/usr/bin/clang
make  %{?_smp_mflags}
make install
cd contrib
make && make install

%install
mkdir -p %{buildroot}/usr/pgsql-%{_pgmajor}/lib
mkdir -p %{buildroot}/usr/pgsql-%{_pgmajor}/include
mkdir -p %{buildroot}/usr/pgsql-%{_pgmajor}/bin
mkdir -p %{buildroot}/usr/pgsql-%{_pgmajor}/share

cp -rp %_topdir/BUILD/%{name}-%{version}/install/lib/* %{buildroot}/usr/pgsql-%{_pgmajor}/lib
cp -rp %_topdir/BUILD/%{name}-%{version}/install/include/* %{buildroot}/usr/pgsql-%{_pgmajor}/include
cp -rp %_topdir/BUILD/%{name}-%{version}/install/bin/* %{buildroot}/usr/pgsql-%{_pgmajor}/bin
cp -rp %_topdir/BUILD/%{name}-%{version}/install/share/* %{buildroot}/usr/pgsql-%{_pgmajor}/share

mkdir -p %{buildroot}%{_sysconfdir}/ld.so.conf.d
/bin/echo "/usr/pgsql-%{_pgmajor}/lib/" > %{buildroot}%{_sysconfdir}/ld.so.conf.d/%{name}-%{_arch}.conf

%pre
/usr/sbin/groupadd -g 26 -o -r postgres >/dev/null 2>&1 || :
/usr/sbin/useradd -M -g postgres -o -r -d /var/lib/pgsql -s /bin/bash \
%if 0%{?rhel} >= 6
-N \
%endif
    -c "PostgreSQL Server" -u 26 postgres >/dev/null 2>&1 || :


%clean
rm -rf %{buildroot}

%post
ldconfig

%postun
ldconfig

%files
%defattr(-,root,root,-)
/usr/pgsql-%{_pgmajor}/lib/*
/usr/pgsql-%{_pgmajor}/include/*
/usr/pgsql-%{_pgmajor}/bin/*
/usr/pgsql-%{_pgmajor}/share/*
%config(noreplace) %_sysconfdir/ld.so.conf.d/%{name}-%{_arch}.conf

%doc



%changelog
