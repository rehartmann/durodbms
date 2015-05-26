current_db := 'SDB';

begin tx;

insert net_actions tup { path '/addsupp', opname 'addsupp' };
insert net_actions tup { path '/listsupps', opname 'listsupps' };

-- Action operator for adding a supplier
operator addsupp(model tup same_heading_as(suppliers), view string) updates { model, view };
    begin tx;
    insert suppliers model;
    commit;
end operator;

-- Action operator for list all suppliers in alphabetical order
operator listsupps(model tup { supps array tup same_heading_as(suppliers) }, view string) updates { model, view };
    begin tx;
    load model.supps from suppliers order (sname asc);
    commit;
end operator;

commit;
