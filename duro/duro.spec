Summary: Relational open source database library
Name: duro
Version: 0.8
Release: 1
Source: %{name}-%{version}.tar.gz
Copyright: GPL
Group: System Environment/Libraries
Buildroot: %{_tmppath}/%{name}-root

%description
Duro is a relational open source database library written in C. It is based on the principles laid down in the book Foundation for Future Database Systems: The Third Manifesto by C. J. Date and Hugh Darwen. 

The goal of the Duro project is to create a library which is as compliant with the proposed database language D as it is possible for a C library. This library is supposed to serve as a basis for a true relational database management system (TRDBMS).

%package devel
Summary: Library and header files for duro
Group: Development/Libraries

%description devel
The duro-devel package contains library archives and header files you will need to build applications that utilize the duro library.

%prep
%setup

%build
./configure --prefix=$RPM_BUILD_ROOT/usr
make

%install
[ "%{buildroot}" != "/" ] && rm -rf %{buildroot}
make install

%clean
[ "%{buildroot}" != "/" ] && rm -rf %{buildroot}

%files
%defattr(-,root,root)
%{_libdir}/*.so
%{_libdir}/*.so.*
%{_libdir}/pkgIndex.tcl
%{_libdir}/util.tcl
%{_bindir}/lstables
%{_bindir}/duroadmin.tcl
%doc docs

%files devel
%defattr(-,root,root)
%{_libdir}/libduro.la
%{_libdir}/libduro.a
%{_libdir}/libdurotcl.a
%{_libdir}/libdurotcl.la
%{_includedir}/gen/*
%{_includedir}/rec/*
%{_includedir}/rel/*
%{_includedir}/dli/*


%changelog
* Mon Mar 15 2004 Dan Hanks <hanksdc@plug.org>
- New spec file
