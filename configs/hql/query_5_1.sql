CREATE TABLE catalog.top_10_frequently_purchases_categories
STORED AS ORC
AS
select categoryName, count(*) as purchasesPerCat from catalog.products group by categoryName order by purchasesPerCat desc limit 10;