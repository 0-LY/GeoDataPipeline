from fastapi import FastAPI, HTTPException
from sqlalchemy.ext.asyncio import AsyncSession, create_async_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy import text
from pydantic import BaseModel
import os

# Database Configuration
POSTGRES_USER = os.getenv("POSTGRES_USER")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD")
POSTGRES_HOST = os.getenv("POSTGRES_HOST")
POSTGRES_PORT = os.getenv("POSTGRES_PORT")
POSTGRES_DB = os.getenv("POSTGRES_DB")

DATABASE_URL = f"postgresql+asyncpg://{POSTGRES_USER}:{POSTGRES_PASSWORD}@{POSTGRES_HOST}:{POSTGRES_PORT}/{POSTGRES_DB}"

# Set up async database connection
engine = create_async_engine(DATABASE_URL, echo=True)
async_session = sessionmaker(
    engine, expire_on_commit=False, class_=AsyncSession
)

# Initialize FastAPI
app = FastAPI()

# Pydantic models for responses
class EventPerHour(BaseModel):
    hour: str  # hour will be converted to a string
    event_count: int

class UserPerHour(BaseModel):
    hour: str  # Convert hour to string if it's a datetime
    user_count: int

class LocationPerUser(BaseModel):
    date: str  # Convert date to string
    user_id: str
    location_count: int

class UniqueUsersPerAdminUnit(BaseModel):
    lau_id: str
    lau_name: str
    date: str  # Convert date to string
    unique_user_count: int

# Helper function to execute SQL queries
async def execute_query(query: text, params: dict = None):
    async with async_session() as session:
        result = await session.execute(query, params or {})
        return result.fetchall()

# Endpoint for `events_per_hour`
@app.get("/events_per_hour", response_model=list[EventPerHour])
async def get_events_per_hour():
    query = text("SELECT hour::text, event_count FROM events_per_hour;")  # Ensure hour is text
    try:
        results = await execute_query(query)
        return [EventPerHour(hour=row[0], event_count=row[1]) for row in results]
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

# Endpoint for `users_per_hour`
@app.get("/users_per_hour", response_model=list[UserPerHour])
async def get_users_per_hour():
    query = text("SELECT hour::text, user_count FROM users_per_hour;")  # Ensure hour is text
    try:
        results = await execute_query(query)
        return [UserPerHour(hour=row[0], user_count=row[1]) for row in results]
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

# Endpoint for `locations_per_user`
@app.get("/locations_per_user", response_model=list[LocationPerUser])
async def get_locations_per_user():
    query = text("SELECT date::text, user_id, location_count FROM locations_per_user;")  # Ensure date is text
    try:
        results = await execute_query(query)
        return [LocationPerUser(date=row[0], user_id=row[1], location_count=row[2]) for row in results]
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

# Endpoint for `unique_users_per_admin_unit` with date filter
@app.get("/unique_users_per_admin_unit", response_model=list[UniqueUsersPerAdminUnit])
async def get_unique_users_per_admin_unit(date: str):
    query = text("""
    SELECT lau_id, lau_name, date::text, unique_user_count 
    FROM unique_users_per_admin_unit 
    WHERE date = :date;
    """)  # Ensure date is text
    try:
        results = await execute_query(query, {'date': date})
        return [UniqueUsersPerAdminUnit(
            lau_id=row[0], lau_name=row[1], date=row[2], unique_user_count=row[3]
        ) for row in results]
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
