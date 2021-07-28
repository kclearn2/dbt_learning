with payments as (
    
    select 
        id as payment_id,
        orderid as order_id,
        paymentmethod as payment_method,
        status,
        amount / 100 as amount,
        created AS created_at,
        
    from dbt-tutorial.stripe.payment
)

select * from payments