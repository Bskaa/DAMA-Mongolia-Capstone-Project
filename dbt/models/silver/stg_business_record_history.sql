-- Append-only audit trail of every business record loaded from the Excel workbooks.
select
    load_id::uuid as load_id,
    file_name::text as file_name,
    record_key::bigint as record_key,
    record::jsonb as record,
    loaded_at::timestamptz as loaded_at
from {{ source('bronze', 'business_record_history') }}
