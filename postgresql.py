import sqlalchemy as sqla
import pandas as pd
import datetime

#Put data from AWS RDS after crearting PostgreSQL database - username, password, endpoint
engine = sqla.create_engine('postgresql://<USERNAME>:<PASSWORD>@<DATABASE_ENDPOINT>.amazonaws.com/postgres')

#Below query is used to group apartments offer into 4 categories, depending on total space of single apartments
#Similar to categories used for bankier.pl economic portal - see example:
#https://www.bankier.pl/wiadomosc/Ceny-ofertowe-mieszkan-maj-2022-Raport-Bankier-pl-8342893.html
select_query_group= """SELECT "Market", "SpaceCategory", AVG("MetrePrice") AS "AveragePrice" FROM 
               (SELECT *, CASE
                WHEN "Space" > 90 THEN '90-'
                WHEN "Space" > 60 AND "Space" <= 90 THEN '60-90'
                WHEN "Space" > 38 AND "Space" <= 60 THEN '38-60'
                WHEN "Space" > 0 AND "Space" <= 38 THEN '0-38' ELSE '0' END AS "SpaceCategory"
                FROM apartments) AS a
                GROUP BY "SpaceCategory", "Market"
                ORDER BY "SpaceCategory"
            """
results = pd.DataFrame(engine.execute(select_query_group).fetchall())
results = results.assign(Date=datetime.date.today())

#Results of previous query are added to new table with average values for given month
results.to_sql('apartments_average', engine, schema='public', if_exists='replace', index=False)

select_query_avg = """SELECT * FROM apartments_average"""

results_avg = pd.DataFrame(engine.execute(select_query_avg).fetchall())
print(results_avg)
