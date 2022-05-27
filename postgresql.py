import sqlalchemy as sqla
import pandas as pd
import datetime

#Put data from AWS RDS after crearting PostgreSQL database - username, password, endpoint
engine = sqla.create_engine('postgresql://postgres:polonez12@database-1.cmhslx64rjfk.us-east-1.rds.amazonaws.com/postgres')


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

results.to_sql('apartments_average', engine, schema='public', if_exists='replace', index=False)

select_query_avg = """SELECT * FROM apartments_average"""

results_avg = pd.DataFrame(engine.execute(select_query_avg).fetchall())
print(results_avg)
