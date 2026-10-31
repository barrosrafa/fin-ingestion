select source,event_id,revision_hash,ticker,event_type,payload,observed_at
from {{ source('canonical','corporate_action_latest') }}
