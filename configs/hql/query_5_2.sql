CREATE TABLE catalog.top_10_frequently_purchases_products_by_category
STORED AS ORC
AS
select categoryName, name, productPurchases, rank
from
(select categoryName, name, productPurchases, row_number() over (partition by categoryName order by productPurchases desc) as rank
from
(select categoryName, name, count(*) as productPurchases
  from catalog.products
  group by categoryName, name) t) t2
where rank < 10
order by categoryName, rank;