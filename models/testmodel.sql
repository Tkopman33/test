-- Import CTEs --> Logical CTEs --> Final CTE --> Simple select statement (select * from final)

with customers as (
     select 
        first_name || ' ' || last_name as name, 
        * 
      from {{ref('stg_jaffle_shop__customers')}}
),

ordersequence as (
     select 
        row_number() over (partition by customer_id order by order_date, id) as user_order_seq,
        *
      from {{ref('stg_jaffle_shop__orders')}}
), 

 customer_order_history as (
    select 
        customers.customer_id as customer_id,
        customers.name as full_name,
        customers.last_name as surname,
        customers.first_name as givenname,
        min(order_date) as first_order_date,
        min(case when ordersequence.order_status NOT IN ('returned','return_pending') then order_date end) as first_non_returned_order_date,
        max(case when ordersequence.order_status NOT IN ('returned','return_pending') then order_date end) as most_recent_non_returned_order_date,
        COALESCE(max(user_order_seq),0) as order_count,
        COALESCE(count(case when ordersequence.order_status != 'returned' then 1 end),0) as non_returned_order_count,
        sum(case when ordersequence.order_status NOT IN ('returned','return_pending') then ROUND(payment.payment_amount/100.0,2) else 0 end) as total_lifetime_value,
        sum(case when ordersequence.order_status NOT IN ('returned','return_pending') then ROUND(payment.payment_amount/100.0,2) else 0 end)/NULLIF(count(case when ordersequence.status NOT IN ('returned','return_pending') then 1 end),0) as avg_non_returned_order_value,
        array_agg(distinct ordersequence.order_id) as order_ids

    from ordersequence

    join customers
    on ordersequence.customer_id = customers.customer_id

    left outer join {{ ref('stg_stripe__payment')}} as payment
    on ordersequence.order_id = payment.order_id

    where ordersequence.order_status NOT IN ('pending') and payment.status != 'fail'

    group by customers.customer_id, customers.name, customers.last_name, customers.first_name),
    
    
final as (
    select 
        ordersequence.order_id as order_id,
        ordersequence.customer_id as customer_id,
        last_name as surname,
        first_name as givenname,
        first_order_date,
        order_count,
        total_lifetime_value,
        round(payment_amount/100.0,2) as order_value_dollars,
        orders.order_status as order_status,
        payment.payment_status as payment_status
    from {{ref('stg_jaffle_shop__orders')}} as orders

    join customers 
    on orders.customer_id = customers.customer_id

    join customer_order_history
    on orders.customer_id = customer_order_history.customer_id

    left outer join payment
    on orders.order_id = payment.order_id

    where payment.payment_status != 'fail')

    select * from final