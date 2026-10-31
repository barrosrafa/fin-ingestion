-- Prices are explicitly nominal; multiple sources remain independently identifiable.
select * from {{ source('market','nominal_bar') }}
