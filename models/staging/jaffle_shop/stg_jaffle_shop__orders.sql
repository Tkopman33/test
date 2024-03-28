with customers as (

    select * from {{ source('jaffle_shop', 'orders')}}
)
    select 
        id as order_id,
        user_id as customer_id,
        order_date.
        status as order_status,
        _etl_loaded_at as etl_loaded_at
    from orders
