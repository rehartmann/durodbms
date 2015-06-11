current_db := 'SDB';

begin tx;

insert net_actions tup { path '/addsupp', opname 'addsupp', method 'POST' };
insert net_actions tup { path '/listsupps', opname 'listsupps', method 'GET' };
insert net_actions tup { path '/listsupps', opname 'listsupps', method 'POST' };

-- Action operator for adding a supplier
operator addsupp(model tup same_heading_as(extend suppliers : { msg := '' } ),
        view string) updates { model, view };
    begin tx;
    try
    	insert suppliers model { all but msg };
    	model.msg := 'Supplier ' || model.sname || ' added.';
    catch err key_violation_error;
        model.msg := 'A supplier with sno ' || cast_as_string(model.sno) || ' already exists.';
    end try;
    commit;
end operator;

-- Action operator for listing all suppliers in alphabetical order
operator listsupps(model tup { supps array tup same_heading_as(suppliers) }, view string) updates { model, view };
    begin tx;
    load model.supps from suppliers order (sname asc);
    commit;
end operator;

commit;
