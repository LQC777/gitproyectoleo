Modos de sentecia pyspark
-------------------------------------------------------------------------------------------
0 forma
rm1 = "/data/master/pmol/data/t_pmol_daily_average_balance/"
dateRange = [rm1 + "cutoff_date=2021-07-20"]
sdf1 = sqlContext.read.option("basePath",rm1).parquet(*dateRange)
sdf1 = sdf1.selectExpr("currency_id",
                       "gl_account_id",
                       "gl_branch_id",
                       "gl_register_operation_id",    
                       "customer_id",      
                       "cast(daily_lcur_average_bal_amount as string)",
                       "main_branch_id"
                       )



1 forma

rm2 = "/data/master/pmol/data/t_pmol_daily_balance_aux/"
sdf2 = spark.read.parquet(rm2+"cutoff_date=2021-07-01")

sdf2a = sdf2.where(F.col("customer_id")=="04301722").\
selectExpr(
"cast(calendar_date as string)",\
"analytical_concept_nature_id",\
"contract_detail_id",\
"origin_currency_id",\
"origin_currency_type",\
"process_date_period_id",\
"source_application_id",\
"contract_accounting_branch_id",\
"current_contract_id",\
"gl_account_id",\
"cast(mp_applicative_punctual_amount as string)",\
"cast(rcd_app_punctual_amount as string)",\
"customer_id"
)
-------------------------------------------------------------------------------------------
2 Forma

rm2 = "/data/master/pmol/data/t_pmol_daily_balance_aux/"
sdf2 = spark.read.parquet(rm2)

sdf2a = sdf2.where((sdf2.cutoff_date == "2021-07-01") & (sdf2.customer_id =="04301722")).\
selectExpr(
"cast(calendar_date as string)",\
"analytical_concept_nature_id",\
"contract_detail_id",\
"origin_currency_id",\
"origin_currency_type",\
"process_date_period_id",\
"source_application_id",\
"contract_accounting_branch_id",\
"current_contract_id",\
"gl_account_id",\
"cast(mp_applicative_punctual_amount as string)",\
"cast(rcd_app_punctual_amount as string)",\
"customer_id"
)
-------------------------------------------------------------------------------------------
3 Forma

rm3 = "/data/master/pmol/data/t_pmol_daily_balance_aux/"
sdf3 = spark.read.parquet(rm3)
sdf3a = sdf3.where((sdf3.cutoff_date == "2021-07-01") & (sdf3.customer_id =="04301722"))
sdf3b = sdf3a.selectExpr("cast(calendar_date as string)",
                         "cast(cutoff_date as string)",
                         "analytical_concept_nature_id",
                         "contract_accounting_branch_id",
                         "contract_detail_id",
                         "current_contract_id",
                         "gl_account_id",
                         "origin_currency_id",
                         "origin_currency_type",
                         "process_date_period_id",
                         "source_application_id",
                         "customer_id",
                         "mp_applicative_punctual_amount",
                         "rcd_app_punctual_amount"
                         )
-----------------------------------------------------------------------------------------------                         