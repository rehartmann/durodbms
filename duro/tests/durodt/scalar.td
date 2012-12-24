VAR i integer;
VAR j integer INIT 1;
put_line(cast_as_string(j + 1));
i := 2;
put_line(cast_as_string(i + j));
VAR k SAME_TYPE_AS(i);
k := 5;

VAR c1 string;
c1 := 'AB%&/';
VAR c2 string INIT 'XX';
put_line(c1);
put_line(c2);

VAR b1 boolean;
put(b1); put_line('');
VAR b2 boolean init TRUE;

VAR f1 float;
VAR f2 float init 1.1;
f1 := 1.0;

VAR bin1 binary;
bin1 := X'416a';
put(bin1); put_line('');
VAR bin2 binary init X'2025';
put(bin2); put_line('');

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
    put_line('error caught');
END TRY;
