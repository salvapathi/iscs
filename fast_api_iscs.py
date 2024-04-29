import mysql.connector
import pandas as pd
from fastapi import FastAPI,Request
import uvicorn
import os
from datetime import datetime
import pytz

# Create an instance of FastAPI
app = FastAPI(title="REST API using FastAPI mysql server iscs data")
hostname = os.environ.get('DB_HOSTNAME')
username = os.environ.get('DB_USERNAME')
password = os.environ.get('DB_PASSWORD')
database = os.environ.get('DB_NAME')
#Establish connection to the database
# # Define database credentials


connection = mysql.connector.connect(
    host=hostname,
    user=username,
    password=password,
    database=database
)

# Check if the connection is successful
if connection.is_connected():
    print("Connected to the database")

    # Define a route to execute the SELECT query
    @app.get("/recruitment/")
    def get_recruitment_data(user_name: str,technology: str): #
       
        

       
      
        
        # Create a cursor object
        cursor = connection.cursor()

        # Execute the SELECT query
        cursor.execute("SELECT * FROM Recruitment WHERE `Skill_Technology` = %s", (technology,))
        
        # Fetch column names
        columns = [col[0] for col in cursor.description]
        
        # Fetch rows
        rows = cursor.fetchall()
        
        # Create a DataFrame
        df = pd.DataFrame(rows, columns=columns)
        #timestap
        ist = pytz.timezone('Asia/Kolkata')  # Indian Standard Time
        ist_time = datetime.now(ist)

        # Convert IST time to MySQL datetime format (YYYY-MM-DD HH:MM:SS)
        ist_time_str = ist_time.strftime('%Y-%m-%d %H:%M:%S')
        time_stamp_sql="INSERT INTO customer_login_details (user_name,api_hit_time VALUE (%s,%s)"
        cursor.execute(time_stamp_sql, (user_name,ist_time_str))
        connection.commit()
  
        
        return df.iloc[:, :10].to_dict(orient="records") 
        

    # Close the connection when the application stops
    @app.on_event("shutdown")
    def shutdown_event():
        connection.close()

else:
    print("Failed to connect to the database")
