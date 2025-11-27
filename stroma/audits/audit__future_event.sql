AUDIT (
    name event_not_in_future
);
select * from @this_model
where @column > current_date;


AUDIT (
    name event_not_in_future_non_blocking,
    blocking false
);
select * from @this_model
where @column > current_date;
