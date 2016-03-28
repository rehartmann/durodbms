-- Script to upgrade a database environment
-- Must be run from the source version (0.26, 1.0, or 1.1) 

const dest_major_version 1;
const dest_minor_version 2;

begin tx;
var dbversion init tuple from sys_version_info;
commit;

io.put_line('catalog version is ' || cast_as_string(dbversion.major_version)
        || '.' || cast_as_string(dbversion.minor_version)
        || '.' || cast_as_string(dbversion.micro_version));

if dbversion.major_version > dest_major_version
        or dbversion.major_version = dest_major_version
            and dbversion.minor_version > dest_minor_version then
    io.put_line('db version is greater destination version, exiting');
end if;

if dbversion.major_version = dest_major_version
        and dbversion.minor_version = dest_minor_version then
    io.put_line('db version equals destination version, exiting');
    exit();
end if;

if dbversion.major_version = 0 and dbversion.minor_version < 26 then
    io.put_line('upgrading is not supported, exiting');
    exit();
end if;

io.put_line('upgrading to ' || cast_as_string(dest_major_version)
        || '.' || cast_as_string(dest_minor_version)
        || '.0');

begin tx;

var sys_ro_op_versions real relation {
    opname string,
    version string,
    argtypes array binary,
    lib string,
    symbol string,
    source string,
    rtype binary,
    creation_time datetime }
key { opname, argtypes }
key { opname, version };

var sys_upd_op_versions real relation {
    opname string,
    version string,
    argtypes array binary,
    lib string,
    symbol string,
    source string,
    updv array boolean,
    creation_time datetime }
key { opname, argtypes }
key { opname, version };

var sys_subtype real relation {
    typename string,
    supertypename string
};

update sys_rtables
    where tablename = identifier('sys_ro_op_versions')
    or tablename = identifier('sys_upd_op_versions')
    or tablename = identifier('sys_subtype')
{ is_user := false };

constraint sys_ro_op_versions
    is_empty(sys_ro_ops {opname, argtypes}
             intersect sys_ro_op_versions {opname, argtypes});

constraint sys_upd_op_versions
    is_empty(sys_upd_ops {opname, argtypes}
             intersect sys_upd_op_versions {opname, argtypes});

-- Add new catalog tables to all databases
var dbs array tup { dbname string };
load dbs from sys_dbtables { dbname } order();
var i int;
for i := 0 to length(dbs) - 1;
    insert sys_dbtables tup { tablename identifier('sys_ro_op_versions'), dbname dbs[i].dbname };  
    insert sys_dbtables tup { tablename identifier('sys_upd_op_versions'), dbname dbs[i].dbname };  
    insert sys_dbtables tup { tablename identifier('sys_subtype'), dbname dbs[i].dbname };  
end for;

update sys_version_info {
    major_version := dest_major_version,
    minor_version := dest_minor_version,
    micro_version := 0
};

commit;

io.put_line('success.');
