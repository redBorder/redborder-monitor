%if 0%{?rhel} <= 6
Name:    redborder-monitor
%endif
%if 0%{?rhel} > 6
Name:    redborder-monitor
%endif
Version: %{__version}
Release: %{__release}%{?dist}

License: GNU AGPLv3
URL: https://github.com/redBorder/redborder-monitor
Source0: %{name}-%{version}.tar.gz

BuildRequires: golang

Summary: Get data events via SNMP or scripting and send results in json over kafka.
Group:   Development/Libraries/C and C++
Requires(pre): shadow-utils

%description
%{summary}

%prep
%setup -qn %{name}-%{version}

%build
# Build the Go application
go build -ldflags "-X main.Version=%{version}" -o redborder-monitor ./cmd/redborder-monitor

%install
mkdir -p %{buildroot}/usr/bin
install -p -m 755 redborder-monitor %{buildroot}/usr/bin/redborder-monitor
mkdir -p %{buildroot}/etc/%{name}
install -D -m 644 redborder-monitor.service %{buildroot}/usr/lib/systemd/system/%{name}.service
install -D -m 644 packaging/rpm/config.json %{buildroot}/etc/%{name}/config.json

%clean
rm -rf %{buildroot}

%pre
getent group %{name} >/dev/null || groupadd -r %{name}
getent passwd %{name} >/dev/null || \
    useradd -r -g %{name} -d / -s /sbin/nologin \
    -c "User of %{name} service" %{name}
exit 0

%post -p /sbin/ldconfig
%postun -p /sbin/ldconfig

%files
%defattr(755,root,root)
/usr/bin/redborder-monitor
%defattr(644,root,root)
%config(noreplace) /etc/%{name}/config.json
/usr/lib/systemd/system/%{name}.service

%changelog
* Fri Jul 10 2026 David Vanhoucke <dvanhoucke@redborder.com> - 1.0.0-1
- Go version of redborder-monitor
