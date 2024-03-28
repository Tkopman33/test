with payment as (

    select * from {{ source('stripe', 'payment')}}
)

select
    id as payment_id,
    orderid as order_id,
    status as payment_status,
    amount as payment_amount,
    created as payment_created_date,
    _batched_at as batched_at
from payment