current_db := 'SDB';

begin tx;

insert net_actions tup { path '/addsupp', opname 'addsupp', method 'POST' };
insert net_actions tup { path '/editsupp', opname 'editsupp', method 'GET' };
insert net_actions tup { path '/updatesupp', opname 'updatesupp', method 'POST' };
insert net_actions tup { path '/listsupps', opname 'listsupps', method 'GET' };
insert net_actions tup { path '/askdelsupp', opname 'askdelsupp', method 'GET' };
insert net_actions tup { path '/delsupp', opname 'delsupp', method 'POST' };

-- Action operator for adding a supplier
operator addsupp(model tup same_heading_as(extend suppliers : { msg := '' } ),
        view string) updates { model, view };
    begin tx;
    try
    	d_insert suppliers model { all but msg };
    	model.msg := 'Supplier ' || model.sname || ' added.';
    catch err key_violation_error;
        model.msg := 'A supplier with sno ' || cast_as_string(model.sno) || ' already exists.';
    catch err element_exists_error;
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

-- Action operator for editing a supplier
operator askdelsupp(model tup same_heading_as(extend suppliers : { msg := '' } ),
        view string) updates { model, view };
    begin tx;
    model := (tuple from suppliers where sno = model.sno) union tuple { msg '' };
    commit;
end operator;

operator delsupp(model tup { sno int, sname string, msg string }, view string) updates { model, view };
    begin tx;
    delete suppliers where sno = model.sno;
    model.msg := 'Supplier ' || model.sname || ' deleted.';
    commit;
    view := 'supp';
end operator;

commit;
