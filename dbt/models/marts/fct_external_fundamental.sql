-- Provider fields retain their original names, units and fiscal context.
select source,dataset,record_key,payload,captured_at
from {{ source('canonical','provider_record') }}
