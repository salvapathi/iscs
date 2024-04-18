
import mysql.connector
import pandas as pd
from fastapi import FastAPI
from fastapi.responses import JSONResponse

# Create an instance of FastAPI
app = FastAPI()

# Define database credentials
hostname = "salvadatabase.mysql.database.azure.com"
username = "manapakadatabase"
password = "Salva@123"
database = "iscs"

# Establish connection to the database
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
    def get_recruitment_data(technology: str):
        # Create a cursor object
        cursor = connection.cursor()

        # Execute the SELECT query
        cursor.execute("SELECT * FROM Recruitment WHERE `Skill / Technology` = %s", (technology,))
        
        # Fetch column names
        columns = [col[0] for col in cursor.description]
        
        # Fetch rows
        rows = cursor.fetchall()
        
        # Create a DataFrame
        df = pd.DataFrame(rows, columns=columns)
  
        
        # # Return the DataFrame
        return df.iloc[:, :10].to_dict(orient="records")
        

    # Close the connection when the application stops
    @app.on_event("shutdown")
    def shutdown_event():
        connection.close()

else:
    print("Failed to connect to the database")
