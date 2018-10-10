CREATE DATABASE catalog;

CREATE TABLE catalog.top_10_frequently_purchases_categories (categoryname varchar(1024), purchasespercat int);
CREATE TABLE catalog.top_10_frequently_purchases_products_by_category (categoryname varchar(1024), name varchar(512), productpurchases int);
CREATE TABLE catalog.top_10_countries_with_highest_money_spending (country_name varchar(256), money_spent decimal(16, 2));
