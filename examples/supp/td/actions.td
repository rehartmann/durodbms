current_db := 'SDB';

begin tx;

insert net_actions tup { path '/addsupp', opname 'addsupp', method 'POST' };
insert net_actions tup { path '/editsupp', opname 'editsupp', method 'GET' };
insert net_actions tup { path '/updatesupp', opname 'updatesupp', method 'POST' };
insert net_actions tup { path '/listsupps', opname 'listsupps', method 'GET' };

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
    view := 'supp';
end operator;

-- Action operator for editing a supplier
operator editsupp(model tup same_heading_as(extend suppliers : { msg := '' } ),
        view string) updates { model, view };
    begin tx;
    model := (tuple from suppliers where sno = model.sno) union tuple { msg '' };
    commit;
end operator;

-- Action operator for updating a supplier
operator updatesupp(model tup same_heading_as(extend suppliers : { msg := '' } ),
        view string) updates { model, view };
    begin tx;
    update suppliers where sno = model.sno {
            sname := model.sname,
            rating := model.rating,
            city := model.city
    };
   	model.msg := 'Supplier ' || model.sname || ' updated.';
    commit;
    view := 'supp';
end operator;

-- Action operator for listing all suppliers in alphabetical order
operator listsupps(model tup { supps array tup same_heading_as(suppliers) }, view string) updates { model, view };
    begin tx;
    load model.supps from suppliers order (sname asc);
    commit;
end operator;

commit;
