-- Failure injection for the end-to-end acceptance run only: returns a row (fails) when
-- --vars '{v2_inject_test_failure: true}' is passed. Passes otherwise.
select 'injected dbt test failure' as reason
where {{ 'true' if var('v2_inject_test_failure') else 'false' }}
