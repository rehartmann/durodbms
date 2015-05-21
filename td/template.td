begin tx;

try
    package net;
    drop type template_error;
    end package;
catch err type_not_found_error;
    ;
end try;
try
    package net;
    drop operator get_cdir;
    end package;
catch err operator_not_found_error;
    ;
end try;
try
    package net;
    drop operator eat_opening_cdir;
    end package;
catch err operator_not_found_error;
    ;
end try;
try
    package net;
    drop operator process_control;
    end package;
catch err operator_not_found_error;
    ;
end try;
try
    package net;
    drop operator process_template;
    end package;
catch err operator_not_found_error;
    ;
end try;
try
    package net;
    drop operator template_to_op;
    end package;
catch err operator_not_found_error;
    ;
end try;

package net;

type template_error
    possrep { msg string }
    init net.template_error('');

-- Drop operators if they already exist
operator get_cdir(current_line string, pos int) returns string;
    var endc init strfind_b(current_line, '$', pos);
    if endc = -1 then
        raise template_error('control directive without proper delimiter');
    end if;
    var endw init strfind_b(current_line, ' ', pos);
    if (endw <> -1) and (endw < endc) then
        endc := endw;
    end if;
    return substr_b(current_line, pos, endc - pos);
end operator;

operator eat_opening_cdir(current_line string, pos int) updates {pos};
    if pos + 3 > strlen_b(current_line) then
        raise template_error('missing {$%end or {$%else');
    end if;
    if substr_b(current_line, pos, 3) <> '{$%' then
        raise template_error('missing missing {$%end or {$%else');
    end if;
    pos := pos + 3;
end operator;

operator process_control(istrm io.iostream_id, ostrm io.iostream_id,
                current_line string, pos int, cdir string)
        updates {current_line, pos};
    var cexp string;
    var dirend init strfind_b(current_line, '$}', pos);
    case
        when cdir = 'if' then
            cexp := substr_b(current_line, pos, dirend - pos);
            io.put(ostrm, 'IF ' || cexp || ' THEN ');
            pos := dirend + 2;
            process_template(istrm, ostrm, current_line, pos);
            eat_opening_cdir(current_line, pos);
            cdir := net.get_cdir(current_line, pos);
            case
            when cdir = 'end' then
                io.put(ostrm, 'END IF;');
                pos := strfind_b(current_line, '$}', pos);
                if pos = -1 then
                    raise template_error('missing $}');
                end if;
                pos := pos + 2;
            when cdir = 'else' then
                io.put(ostrm, 'ELSE ');
                pos := strfind_b(current_line, '$}', pos);
                if pos = -1 then
                    raise template_error('missing $}');
                end if;
                pos := pos + 2;
                process_template(istrm, ostrm, current_line, pos);
                eat_opening_cdir(current_line, pos);
                cdir := net.get_cdir(current_line, pos);
                if cdir <> 'end' then
                    raise template_error('encountered {$%' || cdir || ', expected ${%end ');
                end if;
                pos := strfind_b(current_line, '$}', pos);
                if pos = -1 then
                    raise template_error('missing $}');
                end if;
                pos := pos + 2;
                io.put(ostrm, 'END IF;');
            else
                raise template_error('encountered {$%' || cdir || ', expected ${%end or ${%else');
            end case;
        when cdir = 'loop' then
            var endexp init strfind_b(current_line, ' var ', pos);
            if (endexp = -1) or (endexp > dirend) then
                raise template_error('missing var in {$%loop');
            end if;

            cexp := substr_b(current_line, pos, endexp - pos);
            var varname init substr_b(current_line, endexp + 5, dirend - endexp -5);
            io.put(ostrm, 'VAR ' || varname || ' int; ');
            io.put(ostrm, 'FOR ' || varname || ' := 0 TO (' || cexp || ') - 1;');

            pos := dirend + 2;
            process_template(istrm, ostrm, current_line, pos);
            eat_opening_cdir(current_line, pos);
            cdir := net.get_cdir(current_line, pos);
            if cdir <> 'end' then
                raise template_error('encountered {$%' || cdir || ', expected ${%end ');
            end if;
            pos := strfind_b(current_line, '$}', pos);
            if pos = -1 then
                raise template_error('missing $}');
            end if;
            pos := pos + 2;
            io.put(ostrm, 'END FOR;');
        else
            raise template_error('invalid control directive {$%' || cdir);
    end case;
end operator;

operator process_template(istrm io.iostream_id, ostrm io.iostream_id,
        current_line string, pos int) updates {current_line, pos};
    var dirpos int;
    var dirend int;

    while true;
       dirpos := strfind_b(current_line, '{$', pos);
       if dirpos >= 0 then
           -- Directive found
           -- Output text before the directive
           if dirpos > pos then
               io.put(ostrm, "net.put('"
                       || substr_b(current_line, pos, dirpos - pos)
                       || "');");
           end if;

           -- Check if it's a control directive
           pos := dirpos + 2;
           if substr_b(current_line, pos, 1) = '%' then
               pos := pos + 1;
               var cdir init net.get_cdir(current_line, pos);
               if cdir = 'end' or cdir = 'else' then
                   pos := dirpos;
                   return;
               end if;
               pos := pos + strlen(cdir);
               process_control(istrm, ostrm, current_line, pos, cdir);
           else
               -- Extract expression
               dirend := strfind_b(current_line, '$}', dirpos);
	           if dirend = -1 then
	               raise template_error('end of directive not found (must be on the same line)');
	           end if;

	           io.put(ostrm, "net.put("
	                   || substr_b(current_line, pos, dirend - pos)
	                   || ');');
	           pos := dirend + 2;
	       end if;
	   else
	       -- No directive
	       -- Output the remaining line
	       var s init substr_b(current_line, pos);
           io.put_line(ostrm, "net.put_line('" || s || "');");

           -- get next line
           io.get_line(istrm, current_line);
           if io.eof(istrm) then
               leave;
           end if;
           pos := 0;           
	   end if;
    end while;
end operator;

operator template_to_op (tfilename string, view_op string, ostrm io.iostream_id) updates {};
    var istrm io.iostream_id;
    var line string;

    io.put(ostrm, 'PACKAGE t; ');
    io.put(ostrm, 'OPERATOR ' || view_op || ' (m TUPLE { * }) UPDATES {};');

    io.open(istrm, tfilename, 'r');

    io.get_line(istrm, line);
    var pos init 0;
    process_template(istrm, ostrm, line, pos);

    io.close(istrm);
    io.put_line(ostrm, 'END OPERATOR;');
    io.put_line(ostrm, 'END PACKAGE;');
end operator;

end package;

implement package net;
    implement type template_error;
    end implement;
end implement;

commit;
