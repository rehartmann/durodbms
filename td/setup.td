begin tx;

-- Actions table
var net_actions real relation {
    path string,
    method string,
    opname string
} key { path, method };

-- HTTP response operators
package net;

operator send_headers() updates {};
    response_headers_state := 1;

    if response_status <> '' then
        net.put_line('Status: ' || response_status);
    end if;

    var i int;
    for i := 0 to length(response_headers) - 1;
        net.put_line(response_headers[i]);
    end for;

    net.put_line('');
    response_headers_state := 2;
end operator;

operator set_status(status int, reason string) updates {};
    response_status := cast_as_string(status) || ' ' || reason;
end operator;

operator set_status(status int) updates {};
    set_status(status, net.status_reason(status));
end operator;

operator set_response_header(name string, value string) updates {};
    var found init false;
    var namex init name || ': ';

    var i int;
    for i := 0 to length(response_headers) - 1;
        if starts_with(response_headers[i], namex) then
            -- Replace header
            response_headers[i] := namex || value;
            found := true;
        end if;
    end for;
    
    if not found then
        add_response_header(name, value);
    end if;
end operator;

operator add_response_header(name string, value string) updates {};
    var hlen init length(response_headers);

    length(response_headers) := hlen + 1;
    response_headers[hlen] := name || ': ' || value;
end operator;    

end package;

commit;
