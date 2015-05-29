begin tx;

-- Actions table
var net_actions real relation { path string, opname string } key { path };

-- HTTP response operators
package net;

operator send_headers() updates {};
    if not resp_headers_sent then
        resp_headers_sent := true;

        if resp_status <> '' then
            net.put_line('Status: ' || resp_status);
        end if;

        var i int;
        for i := 0 to length(resp_headers) - 1;
            net.put_line(resp_headers[i]);
        end for;

        net.put_line('');
    end if;
end operator;

operator set_status(status int) updates {};
    resp_status := cast_as_string(status) || ' ' || net.status_reason(status);
end operator;

operator set_header(name string, value string) updates {};
    var found init false;
    var namex init name || ': ';

    var i int;
    for i := 0 to length(resp_headers) - 1;
        if starts_with(resp_headers[i], namex) then
            -- Replace header
            resp_headers[i] := namex || value;
            found := true;
        end if;
    end for;
    
    if not found then
        add_header(name, value);
    end if;
end operator;

operator add_header(name string, value string) updates {};
    var hlen init length(resp_headers);

    length(resp_headers) := hlen + 1;
    resp_headers[hlen] := name || ': ' || value;
end operator;    

end package;

commit;
