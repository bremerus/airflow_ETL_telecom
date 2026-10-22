use master

if exists (select 1 from sys.databases where name = 'Telecom_DataWarehouse')
begin
	alter database Telecom_DataWarehouse set single_user with rollback immediate;
	drop database Telecom_DataWarehouse;
end;

go

create database Telecom_DataWarehouse;

go

use Telecom_DataWarehouse

go 

create schema silver;