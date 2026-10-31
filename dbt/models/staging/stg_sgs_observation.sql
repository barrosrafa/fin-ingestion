-- Official SGS observations, normalized and deduplicated before serving.
select reference_date as data_referencia,series_name as serie_nome,
    series_code as serie_id,value as valor_taxa
from {{ source('market','sgs_observation') }}
