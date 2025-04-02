with subscription_state_changes as (
select * from {{ source('customers_transactions_db', 'subscription_state_changes') }}
)

,customers as (
select * from {{ source('customers_transactions_db', 'customers') }}
)

,transactions as (
select * from {{ source('customers_transactions_db', 'transactions') }}
)

,cancellations as (
    select 
    subscription_id
    ,max(case when end_state = 'inactive' then subscription_change_timestamp end) as cancel_timestamp
    from subscription_state_changes
    group by 1
)

,sub_start_end as (
    select 
    subscription_change_id
    ,subscription_state_changes.subscription_id
    ,customer_id
    ,subscription_change_timestamp
    ,cancel_timestamp
    ,lead(subscription_change_timestamp) over (partition by customer_id order by subscription_change_timestamp asc) as next_subscription_start
    from subscription_state_changes
    inner join cancellations on cancellations.subscription_id = subscription_state_changes.subscription_id
    where subscription_change_timestamp <> cancel_timestamp or cancel_timestamp is null
)

,subscription_summary as (
    select
    sub_start_end.customer_id
    ,subscription_id
    ,subscription_change_timestamp as subscription_start_timestamp
    ,coalesce(next_subscription_start, cancel_timestamp) - interval '1 second' as subscription_end_timestamp
    ,lag(subscription_id) over (partition by sub_start_end.customer_id order by subscription_start_timestamp asc) as prior_subscription_id
    from sub_start_end
)

,entry_transaction as (
    select
    transactions.customer_id
    ,first_transaction.min_transaction as first_transaction_timestamp
    ,transactions.category as first_transaction_category
    from transactions
    inner join
    (
        select
        customer_id
        ,min(transaction_timestamp) as min_transaction
        from transactions
        group by 1
    ) first_transaction on transactions.transaction_timestamp = first_transaction.min_transaction
        and transactions.customer_id = first_transaction.customer_id
)

select 
customers.customer_id
,customers.created_at as customer_created_timestamp
,entry_transaction.first_transaction_timestamp as acquisition_timestamp
,entry_transaction.first_transaction_category as acquisition_type
,subscription_summary.subscription_id
,subscription_summary.subscription_start_timestamp
,subscription_summary.subscription_end_timestamp
,subscription_summary.prior_subscription_id
from customers
left join subscription_summary on customers.customer_id = subscription_summary.customer_id
left join entry_transaction on customers.customer_id = entry_transaction.customer_id