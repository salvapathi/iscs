import mysql.connector
import uvicorn
import os
import pandas as pd
from datetime import datetime
import pytz
from typing import Union, Optional, List
from fastapi import FastAPI, Depends, status, Request ,HTTPException
from fastapi.security import HTTPBasic ,HTTPBasicCredentials
from pydantic import BaseModel , ValidationError
from mysql.connector import Error
from pydantic import Field ,validator
from enum import Enum
import re
from fastapi.responses import JSONResponse




# Create an instance of FastAPI
app = FastAPI(title=" FastAPI mysql server iscs Recruiter data viewer")
security = HTTPBasic()
hostname = os.environ.get('DB_HOSTNAME')
username = os.environ.get('DB_USERNAME')
password = os.environ.get('DB_PASSWORD')
database = os.environ.get('DB_NAME')
# ADMIN_USERNAME = os.getenv("ADMIN_USERNAME")
# ADMIN_PASSWORD = os.getenv("ADMIN_PASSWORD")

# ADMIN_USERNAME = "salva"
# ADMIN_PASSWORD = "pass"

# # Define database credentials

def connection_db():
     
     return mysql.connector.connect(
        host=hostname,
        user=username,
        password=password,
        database=database
    )
connection=connection_db()

# if not ADMIN_USERNAME or not ADMIN_PASSWORD:
#     raise Exception("Admin credentials not set in environment variables")

#authentications
# def authenticate(credentials: HTTPBasicCredentials = Depends(security)):
#     if credentials.username == ADMIN_USERNAME and credentials.password == ADMIN_PASSWORD:
#         return True
#     else:
#         raise HTTPException(status_code=401, detail="Unauthorized")
    
class ColumnName(str, Enum):
    Name_of_Candidate = "Name_of_Candidate"
    Mobile_No = "Mobile_No"
    skill_technology = "skill_technology"
    Recruiter="Recruiter"

# Define a route to execute the SELECT query
@app.get("/recruitment_data_view/",summary="Get Recruitment data", description="Retrieve recruitment data based on technology")
def get_recruitment_data(column_name: ColumnName,user_name: str,enter_data: Union[str, int]):#,authenticated: bool = Depends(authenticate)
    user_name = user_name.lower()
    cursor = connection.cursor()
    # Execute the SELECT query and get the records
    try :
        if column_name==ColumnName.Mobile_No:
            Mobile_No = enter_data
            if not Mobile_No.isdigit() or len(Mobile_No) != 10:
                raise HTTPException(status_code=400, detail="Invalid phone number Please enter a valid phone number")   
            else:
                cursor.execute("SELECT * FROM Recruitment WHERE Mobile_No = %s", (Mobile_No,))
                columns = [col[0] for col in cursor.description]
                rows = cursor.fetchall()
                if len(rows) == 0:
                    # No data found for the given query
                    message = f"No data available for {column_name.value} '{enter_data}'"
                    result= JSONResponse(content={"message": message}, status_code=404)
                else:
                    # Data found, proceed with processing
                    df = pd.DataFrame(rows, columns=columns)
                    result = df.iloc[:, :12].to_dict(orient="records")
                    
                
                
        else:
            try:
                enter_data = enter_data.lower()
                where_clause = f"{column_name.value} LIKE %s COLLATE utf8mb4_general_ci"
                sql_query = f"SELECT * FROM Recruitment WHERE {where_clause}"
                cursor.execute(sql_query, (f"{enter_data}%",))
                columns = [col[0] for col in cursor.description]
                rows = cursor.fetchall()
                if len(rows) == 0:
                    # No data found for the given query
                    message = f"No data available for {column_name.value} '{enter_data}'"
                    result= JSONResponse(content={"message": message}, status_code=404)
                else:
                    # Data found, proceed with processing
                    df = pd.DataFrame(rows, columns=columns)
                    result = df.iloc[:, :12].to_dict(orient="records")
                    
                
            except ValueError as ve:
                # Log the error or handle it as needed
                result ("error in the data")
                error_message = "An error occurred while processing the request. in the data filter"
                raise HTTPException(status_code=500, detail=error_message)
                
                
            
        return result

    except mysql.connector.Error as err:
        raise HTTPException(status_code=500, detail=f"Database error: {err} please check database")
    finally:
        print("the data base is still connected and ready to fetch the data ")
        ist = pytz.timezone('Asia/Kolkata')  # Indian Standard Time
        ist_time = datetime.now(ist)
        operation=f"fecting data done by {user_name}"
        # Convert IST time to MySQL datetime format (YYYY-MM-DD HH:MM:SS)
        ist_time_str = ist_time.strftime('%Y-%m-%d %H:%M:%S')
        time_stamp_sql="INSERT INTO customer_login_details (user_name,api_hit_time,operation) VALUE (%s,%s,%s)"
        cursor.execute(time_stamp_sql,(user_name,ist_time_str,operation))
        connection.commit()
    
    
        
 ## data ingestions       

class Record(BaseModel):
    Name_of_Candidate: str = Field(None, description="Name of the Candidate")
    Skill_Technology: str = Field(None,description="Skill or technology")
    Mobile_No: int = Field(None, description="Mobile Number")
    Email_ID: str = Field(None, description="Email ID")
    Recruiter: str = Field(None, description="Name of the RECRUITER")
    
    @validator("Name_of_Candidate")
    def validate_candidate_name(cls, v):
        if not v:
            raise HTTPException(status_code=400, detail="Candidate name cannot be empty")
        return v

    @validator("Skill_Technology")
    def validate_skill_technology(cls, v):
        if not v:
            raise HTTPException(status_code=400, detail="Skill or technology cannot be empty")
        return v

    @validator("Recruiter")
    def validate_recruiter(cls, v):
        if not v:
            raise HTTPException(status_code=400, detail="Recruiter name cannot be empty")
        return v
    @validator("Mobile_No")
    def validate_mobile_number(cls, v):
        if v is None:
            return None
        v = str(v)
        if not re.match("^[0-9]{10}$", v):
            raise HTTPException(status_code=400, detail="Mobile number must be 10 digits long and contain only digits")
        return v

    @validator("Email_ID")
    def validate_email(cls, v):
        if v is None:
            return None
        v = str(v)
        if not re.match(r"^[a-zA-Z0-9_.+-]+@[a-zA-Z0-9-]+\.[a-zA-Z0-9-.]+$", v):
            raise HTTPException(status_code=400, detail="Invalid email format")
        return v

@app.post("/Record",summary="Ingest data", description="Ingest data into the records table")
def data_ingestion(record:Record,connection = Depends(connection_db)):#,authenticated: bool = Depends(authenticate)
        try :
            if connection is not None:
                cursor = connection.cursor()
                ist = pytz.timezone('Asia/Kolkata')  # Indian Standard Time
                ist_time = datetime.now(ist)
                
                # Convert IST time to MySQL datetime format (YYYY-MM-DD HH:MM:SS)
                ist_time_str = ist_time.strftime('%Y-%m-%d %H:%M:%S')
                
                record_dict = record.dict()
                record_dict["Ingested_date"] = ist_time_str

                # Execute the SQL query to insert the record into the database
                # Assuming connection and cursor are available
                cursor.execute("INSERT INTO Recruitment (Name_of_Candidate, Skill_Technology, Mobile_No, Email_ID, Recruiter, Ingested_date) VALUES (%s, %s, %s, %s, %s, %s)",
                                (record_dict["Name_of_Candidate"], record_dict["Skill_Technology"], record_dict["Mobile_No"], record_dict["Email_ID"], record_dict["Recruiter"], record_dict["Ingested_date"]))
                        
                connection.commit()
                print("the data base is connected and the data ingestion is done")
                operation=f"data ingested(post operation) done by {record.Recruiter}"
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
