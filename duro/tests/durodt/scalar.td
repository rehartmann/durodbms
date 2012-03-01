VAR I INTEGER INIT 1;
put_line(CAST_AS_STRING(I + 1));

VAR J SAME_TYPE_AS(I);
J:=2;
put_line(CAST_AS_STRING(I + J));
VAR C CHAR INIT 'XX';
put_line(C);
VAR B BOOLEAN;
put(B); put_line('');

-- Synonyms
VAR B2 BOOL;
VAR I2 INT;
VAR F FLOAT;
VAR F2 RATIONAL;
VAR F3 RAT;

TRY
    VAR I INT;
    VAR I INT;
CATCH ERR ELEMENT_EXISTS_ERROR;
    put_line('error caught');
END TRY;
