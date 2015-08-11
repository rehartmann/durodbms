begin tx;

drop package template;

package template;

type template_error
    possrep { msg string }
    init template.template_error('');

operator get_cdir(current_line string, pos int, lineno int) returns string;
    var endc init strfind_b(current_line, '$', pos);
    if endc = -1 then
        raise template.template_error('control directive without proper delimiter at line '
                || cast_as_string(lineno));
    end if;
    var endw init strfind_b(current_line, ' ', pos);
    if (endw <> -1) and (endw < endc) then
        endc := endw;
    end if;
    return substr_b(current_line, pos, endc - pos);
end operator;

operator eat_opening_cdir(current_line string, pos int, lineno int) updates {pos};
    if pos + 3 > strlen_b(current_line) then
        raise template.template_error('missing {$%end or {$%else at line '
                || cast_as_string(lineno));
    end if;
    if substr_b(current_line, pos, 3) <> '{$%' then
        raise template.template_error('missing missing {$%end or {$%else at line '
                || cast_as_string(lineno));
    end if;
    pos := pos + 3;
end operator;

operator process_control(istrm io.iostream_id, ostrm io.iostream_id,
                current_line string, pos int, cdir string, lineno int)
        updates {current_line, pos};
    var cexp string;
    var dirend init strfind_b(current_line, '$}', pos);
    case
        when cdir = 'if' then
            cexp := substr_b(current_line, pos, dirend - pos);
            io.put(ostrm, 'IF ' || cexp || ' THEN ');
            pos := dirend + 2;
            process_template(istrm, ostrm, current_line, pos, lineno);
            eat_opening_cdir(current_line, pos, lineno);
            cdir := template.get_cdir(current_line, pos, lineno);
            case
            when cdir = 'end' then
                io.put(ostrm, 'END IF;');
                pos := strfind_b(current_line, '$}', pos);
                if pos = -1 then
                    raise template.template_error('missing $} at line '
                            || cast_as_string(lineno));
                end if;
                pos := pos + 2;
            when cdir = 'else' then
                io.put(ostrm, 'ELSE ');
                pos := strfind_b(current_line, '$}', pos);
                if pos = -1 then
                    raise template.template_error('missing $} at line '
                            || cast_as_string(lineno));
                end if;
                pos := pos + 2;
                process_template(istrm, ostrm, current_line, pos, lineno);
                eat_opening_cdir(current_line, pos, lineno);
                cdir := template.get_cdir(current_line, pos, lineno);
                if cdir <> 'end' then
                    raise template.template_error(
                            'encountered {$%' || cdir || ', expected ${%end at line '
                            || cast_as_string(lineno));
                end if;
                pos := strfind_b(current_line, '$}', pos);
                if pos = -1 then
                    raise template.template_error('missing $} at line '
                            || cast_as_string(lineno));
                end if;
                pos := pos + 2;
                io.put(ostrm, 'END IF;');
            else
                raise template.template_error(
                        'encountered {$%' || cdir || ', expected ${%end or ${%else at line '
                        || cast_as_string(lineno));
            end case;
        when cdir = 'loop' then
            var endexp init strfind_b(current_line, ' var ', pos);
            if (endexp = -1) or (endexp > dirend) then
                raise template.template_error('missing var in {$%loop at line '
                        || cast_as_string(lineno));
            end if;

            cexp := substr_b(current_line, pos, endexp - pos);
            var varname init substr_b(current_line, endexp + 5, dirend - endexp -5);
            io.put(ostrm, 'VAR ' || varname || ' int; ');
            io.put(ostrm, 'FOR ' || varname || ' := 0 TO (' || cexp || ') - 1;');

            pos := dirend + 2;
            process_template(istrm, ostrm, current_line, pos, lineno);
            eat_opening_cdir(current_line, pos, lineno);
            cdir := template.get_cdir(current_line, pos, lineno);
            if cdir <> 'end' then
                raise template.template_error(
                        'encountered {$%' || cdir || ', expected ${%end at line '
                        || cast_as_string(lineno));
            end if;
            pos := strfind_b(current_line, '$}', pos);
            if pos = -1 then
                raise template.template_error('missing $} at line '
                        || cast_as_string(lineno));
            end if;
            pos := pos + 2;
            io.put(ostrm, 'END FOR;');
        else
            raise template.template_error('invalid control directive {$%'
                    || cdir || ' at line ' || cast_as_string(lineno));
    end case;
end operator;

operator process_template(istrm io.iostream_id, ostrm io.iostream_id,
        current_line string, pos int, lineno int)
        updates { current_line, pos, lineno };
    var dirpos int;
    var dirend int;

    while true;
       dirpos := strfind_b(current_line, '{$', pos);
       if dirpos >= 0 then
           -- Directive found
           -- Output text before the directive
           if dirpos > pos then
               io.put(ostrm, "http.put(resp, '"
                       || substr_b(current_line, pos, dirpos - pos)
                       || "');");
           end if;

           -- Check if it's a control directive
           dirend := strfind_b(current_line, '$}', dirpos + 2);
           if dirend = -1 then
	           raise template.template_error(
	                   'end of directive not found (must be on the same line) at line '
                       || cast_as_string(lineno));
	       end if;
           pos := dirpos + 2;
           case
           when substr_b(current_line, pos, 1) = '%' then
               pos := pos + 1;
               var cdir init template.get_cdir(current_line, pos, lineno);
               if cdir = 'end' or cdir = 'else' then
                   pos := dirpos;
                   return;
               end if;
               pos := pos + strlen(cdir);
               process_control(istrm, ostrm, current_line, pos, cdir, lineno);
           when substr_b(current_line, pos, 1) = '=' then
               -- Extract expression, no escape
	           io.put(ostrm, "http.put(resp, "
	                   || substr_b(current_line, pos + 1, dirend - pos - 1)
	                   || ');');
	           pos := dirend + 2;
           else
               -- Extract expression, with escape
	           io.put(ostrm, "http.put(resp, net.hescape("
	               || substr_b(current_line, pos, dirend - pos)
	               || '));');
	           pos := dirend + 2;
	       end case;
	   else
	       -- No directive
	       -- Output the remaining line
	       var s init substr_b(current_line, pos);
           io.put_line(ostrm, "http.put_line(resp, '" || s || "');");

           -- get next line
           io.get_line(istrm, current_line);
           if io.eof(istrm) then
               leave;
           end if;
           lineno := lineno + 1;
           pos := 0;           
	   end if;
    end while;
end operator;

operator template_to_op (tfilename string, view_op string, ostrm io.iostream_id) updates {};
    var istrm io.iostream_id;
    var line string;

    io.put(ostrm, 'PACKAGE template; PACKAGE t; ');
    io.put(ostrm, 'OPERATOR ' || view_op || ' (resp http.http_response, m TUPLE { * }) UPDATES { resp };');

    io.open(istrm, tfilename, 'r');

    io.get_line(istrm, line);
    var lineno init 1;
    var pos init 0;
    process_template(istrm, ostrm, line, pos, lineno);

    io.close(istrm);
    io.put_line(ostrm, 'END OPERATOR;');
    io.put_line(ostrm, 'END PACKAGE;');
    io.put_line(ostrm, 'END PACKAGE;');
end operator;

end package;

implement package template;
    implement type template_error;
    end implement;
end implement;

commit;
