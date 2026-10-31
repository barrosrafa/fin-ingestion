select * from {{ ref('fct_nominal_bar') }}
where close <= 0 or low > close or high < close or volume < 0
