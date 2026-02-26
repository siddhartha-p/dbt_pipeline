"""
Employee model mapping to gold.dim_employees table.
"""
from sqlalchemy import Column, String, Integer, Boolean, Date, Numeric, DateTime
from api.database import Base


class Employee(Base):
    """
    Employee model mapping to gold.dim_employees table.
    This is the analytics-ready employee dimension table.
    """
    
    __tablename__ = "dim_employees"
    __table_args__ = {"schema": "gold"}
    
    client_employee_id = Column(String, primary_key=True)
    fullname = Column(String)
    work_email = Column(String)
    dob = Column(Date)
    years_of_experience = Column(Numeric(4, 1))
    job_code = Column(String)
    job_title = Column(String)
    organization_name = Column(String)
    department_name = Column(String)
    manager_employee_id = Column(String)
    job_start_date = Column(Date)
    hire_date = Column(Date)
    recent_hire_date = Column(Date)
    anniversary_date = Column(Date)
    scheduled_weekly_hour = Column(Integer)
    term_date = Column(Date, nullable=True)
    tenure = Column(Integer)  # Calculated field: tenure in years
    is_active = Column(Boolean)
    is_rehired = Column(Boolean)
    is_early_attrition = Column(Boolean)
    loaded_at = Column(DateTime)
    source_file = Column(String)
