import mysql.connector
import uvicorn
import os
import pandas as pd
from datetime import datetime
import pytz
from fastapi import FastAPI, Depends, status, Request ,HTTPException
from fastapi.security import HTTPBasic ,HTTPBasicCredentials
from pydantic import BaseModel
from mysql.connector import Error
from pydantic import Field



# Create an instance of FastAPI
app = FastAPI(title="getting get using FastAPI mysql server iscs data")
hostname = os.environ.get('DB_HOSTNAME')
username = os.environ.get('DB_USERNAME')
password = os.environ.get('DB_PASSWORD')
database = os.environ.get('DB_NAME')


# # Define database credentials

def connection_db():
     return mysql.connector.connect(
        host=hostname,
        user=username,
        password=password,
        database=database
    )

connection=connection_db()
   
def authenticate(credentials: HTTPBasicCredentials = Depends(HTTPBasic())):

    user_credentials = {
    "salva123": "password",
    "user2": "alpha",
    "user3": "beta"
    }
    if credentials.username not in user_credentials or user_credentials[credentials.username] != credentials.password:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Incorrect email or password",
            headers={"WWW-Authenticate": "Basic"},
        )
    return True

# Check if the connection is successful


# Define a route to execute the SELECT query
@app.get("/recruitment_data_view/",summary="Get recruitment data", description="Retrieve recruitment data based on technology")
def get_recruitment_data(user_name: str,technology: str,credentials: HTTPBasicCredentials = Depends(authenticate)): 
    
    user_name = user_name.lower()
    technology=technology.lower()
    # Create a cursor object
    cursor = connection.cursor()


    # Execute the SELECT query
    try :
        cursor.execute("SELECT * FROM Recruitment WHERE `Skill_Technology` LIKE '{}%'".format(technology))
        
        # Fetch column names
        columns = [col[0] for col in cursor.description]
        
        # Fetch rows
        rows = cursor.fetchall()
        
        # Create a DataFrame
        df = pd.DataFrame(rows, columns=columns)
        #timestap
        ist = pytz.timezone('Asia/Kolkata')  # Indian Standard Time
        ist_time = datetime.now(ist)
        operation="fecting data(get data)"
        # Convert IST time to MySQL datetime format (YYYY-MM-DD HH:MM:SS)
        ist_time_str = ist_time.strftime('%Y-%m-%d %H:%M:%S')
        time_stamp_sql="INSERT INTO customer_login_details (user_name,api_hit_time,operation) VALUE (%s,%s,%s)"
        cursor.execute(time_stamp_sql,(user_name,ist_time_str,operation))
        connection.commit()
        print("the data base is connected and ready to fetch the  data ")
        return df.iloc[:, :10].to_dict(orient="records")
    finally:
        print("the data base is still connected and ready to fetch the data ")

    


class Record(BaseModel):
    Name_of_Candidate: str = Field(..., description="Name of the candidate")
    Skill_Technology: str = Field(..., description="Skill or technology")
    Mobile_No : int = Field(..., description="Mobile number")
    Email_ID : str = Field(..., description="Email ID")
    Recruiter : str = Field(..., description="Recruiter name")

@app.post("/Record",summary="Ingest data", description="Ingest data into the records table")
def data_ingestion(record:Record,connection = Depends(connection_db),credentials: HTTPBasicCredentials = Depends(authenticate)):
        try :
            if connection is not None:
                cursor = connection.cursor()
                cursor.execute("INSERT INTO Recruitment (Name_of_Candidate, Skill_Technology, Mobile_No, Email_ID, Recruiter) VALUES (%s, %s, %s, %s, %s)",
                    (record.Name_of_Candidate, record.Skill_Technology, record.Mobile_No, record.Email_ID, record.Recruiter))
                connection.commit()
                print("the data base is connected and the data ingestion is done")
                ist = pytz.timezone('Asia/Kolkata')  # Indian Standard Time
                ist_time = datetime.now(ist)
                operation="data ingested(post operation)"

                # Convert IST time to MySQL datetime format (YYYY-MM-DD HH:MM:SS)
                ist_time_str = ist_time.strftime('%Y-%m-%d %H:%M:%S')
                time_stamp_sql="INSERT INTO customer_login_details (user_name,api_hit_time,operation) VALUE (%s,%s,%s)"
                cursor.execute(time_stamp_sql, (record.Recruiter,ist_time_str,operation))
                connection.commit()
                return {"message": f"Data inserted successfully by {record.Recruiter}"}
            
            else:
                print("the data base is not connected")
                return {"message": "Failed to connect to the database"}

            connection.commit()
        except Error as e:
            print("Error while connecting to MySQL please check connection",e)

    

# Close the connection when the application stops
@app.on_event("shutdown")
def shutdown_event():
    connection.close()


