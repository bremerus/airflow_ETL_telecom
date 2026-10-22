if OBJECT_ID('silver.telecom_churn') is not null
	drop table silver.telecom_churn;

go

create table silver.telecom_churn(
	customer_id int primary key,
	gender nvarchar(10),
	senior_citizen tinyint,
	partner tinyint,
	dependents tinyint,
	tenure int null,
	phone_service tinyint,
	multiple_lines tinyint,
	internet_service nvarchar(15),
	online_security tinyint,
	online_backup tinyint,
	device_protection tinyint,
	tech_support tinyint,
	streaming_tv tinyint,
	streaming_movies tinyint,
	contract int,
	paperless_billing tinyint,
	payment_method nvarchar(30),
	monthly_charges float,
	total_charges float,
	signup_date date,
	churn tinyint,
	internal_flag nvarchar(2)
);