-- Create and populate the database SDB for suppliers

create_db('SDB');

current_db:='SDB';

begin tx;

var suppliers real relation {
    sno int,
    sname string,
    rating int,
    city string
} key { sno };

commit;
