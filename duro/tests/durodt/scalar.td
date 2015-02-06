VAR i integer;
VAR j integer INIT 1;
io.put_line(cast_as_string(j + 1));
i := 2;
io.put_line(cast_as_string(i + j));
VAR k SAME_TYPE_AS(i);
k := 5;

VAR c1 string;
c1 := 'AB%&/';
VAR c2 string INIT 'XX';
io.put_line(c1);
io.put_line(c2);

VAR b1 boolean;
io.put(b1); io.put_line('');
VAR b2 boolean init TRUE;

VAR f1 float;
VAR f2 float init 1.1;
f1 := 1.0;

VAR bin1 binary;
bin1 := X'416a';
io.put(bin1); io.put_line('');
VAR bin2 binary init X'2025';
io.put(bin2); io.put_line('');

-- Synonyms
VAR l int;
VAR c3 char; 
VAR b3 bool;
VAR f4 rational;
VAR f5 rat;

TRY
    VAR i INT;
    VAR i INT;
CATCH err element_exists_error;
    io.put_line('error caught');
END TRY;
