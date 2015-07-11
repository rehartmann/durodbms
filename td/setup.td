begin tx;

-- Actions table
var net_actions real relation {
    path string,
    method string,
    opname string
} key { path, method };

package http;

-- Request type

type http_request
possrep {
    method string
    -- headers and body exist only conceptually,
    -- they are accessible only using get_request_header() etc.
}
init http.http_request ('');

-- Response type
type http_response
possrep {
    status string,
    headers array tuple { name string, value string }
    -- body exists only conceptually, not acessible using the operators below
}
init http.http_response ('200 OK', array tuple { name string, value string } ());

--
-- Response operators
--

operator send_headers(resp http.http_response) updates {};
    dreisam_response_headers_state := 1;

    if the_status(resp) <> '' then
        http.put_line('Status: ' || the_status(resp));
    end if;

    var i int;
    for i := 0 to length(the_headers(resp)) - 1;
        http.put(the_headers(resp)[i].name);
        http.put(': ');
        http.put_line(the_headers(resp)[i].value);
    end for;

    http.put_line('');
    dreisam_response_headers_state := 2;
end operator;

operator set_status(resp http.http_response, status int, reason string)
        updates { resp };
    the_status(resp) := cast_as_string(status) || ' ' || reason;
end operator;

operator set_status(resp http.http_response, status int) updates { resp };
    set_status(resp, status, http.status_reason(status));
end operator;

operator set_response_header(resp http.http_response, name string, value string)
        updates {resp};
    var found init false;

    var i int;
    for i := 0 to length(the_headers(resp)) - 1;
        if the_headers(resp)[i].name = name then
            -- Replace header
            the_headers(resp)[i] := tup { name name, value value };
            found := true;
        end if;
    end for;

    if not found then
        add_response_header(resp, name, value);
    end if;
end operator;

operator add_response_header(resp http.http_response, name string, value string)
        updates {resp};
    var hlen init length(the_headers(resp));

    length(the_headers(resp)) := hlen + 1;
    the_headers(resp)[hlen] := tup { name name, value value };
end operator;

operator send_json_response(resp http.http_response, tp tup { * })
        updates { resp };
    var json_data init net.to_json(tp);
    set_response_header(resp, 'Content-Type', 'application/json');
    set_response_header(resp, 'Content-Length', cast_as_string(strlen_b(json_data)));

    http.put(json_data);
end operator;

end package;

implement package http;

implement type http_request;
end implement;

implement type http_response;
end implement;

end implement;

commit;
