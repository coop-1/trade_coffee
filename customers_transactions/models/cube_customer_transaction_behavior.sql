with transactions as (
    select * from {{ source('customers_transactions_db', 'transactions') }}
)

,dim_customer_acquisition as (
    select * from {{ ref('dim_customer_acquisition') }}
)

select 
    date_part('year', transaction_timestamp) as transaction_year
    ,date_part('month', transaction_timestamp) as transaction_month
    ,acquisition_type
    ,date_part('year', acquisition_timestamp) as acquisition_year
    ,date_part('month', acquisition_timestamp) as acquisition_month
    ,case when date_part('year', transaction_timestamp) = date_part('year', acquisition_timestamp) and date_part('month', transaction_timestamp) = date_part('month', acquisition_timestamp) then 'new' else 'existing' end as customer_type
    ,transactions.category
    ,count(distinct transactions.customer_id) as total_customers
    ,count(distinct transaction_id) as total_transactions
    ,sum(net_revenue) as total_net_revenue
    from transactions
    left join 
    (
        select
        customer_id
        ,acquisition_type
        ,max(customer_created_timestamp) as customer_created_timestamp
        ,max(acquisition_timestamp) as acquisition_timestamp
        from dim_customer_acquisition
        group by 1,2
    ) dim_customer_acquisition on transactions.customer_id = dim_customer_acquisition.customer_id
    group by 1,2,3,4,5,6,7
    order by 1 desc, 2 desc