-- Terminal refund timestamps agree with the status and never precede the request.
select refund_id, status, requested_at, succeeded_at, failed_at, failure_code
from {{ ref('stg_refunds') }}
where not (
       (status = 'requested' and succeeded_at is null and failed_at is null and failure_code is null)
    or (status = 'succeeded' and succeeded_at >= requested_at and failed_at is null and failure_code is null)
    or (status = 'failed' and failed_at >= requested_at and succeeded_at is null and failure_code is not null)
)
