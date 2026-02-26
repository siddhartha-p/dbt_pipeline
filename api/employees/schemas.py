"""
Pydantic schemas for Employee API.
Used for request/response validation.
"""
from pydantic import BaseModel, EmailStr, Field
from typing import Optional
from datetime import date, datetime
from decimal import Decimal


class EmployeeBase(BaseModel):
    """Base employee schema with common fields."""
    fullname: str = Field(..., min_length=1, max_length=200)
    work_email: Optional[EmailStr] = None
    dob: Optional[date] = None
    years_of_experience: Optional[Decimal] = Field(None, ge=0, le=99)
    job_code: Optional[str] = None
    job_title: Optional[str] = None
    organization_name: Optional[str] = None
    department_name: Optional[str] = None
    manager_employee_id: Optional[str] = None
    job_start_date: Optional[date] = None
    hire_date: date
    recent_hire_date: Optional[date] = None
    anniversary_date: Optional[date] = None
    scheduled_weekly_hour: Optional[int] = Field(None, ge=0, le=168)


class EmployeeCreate(EmployeeBase):
    """Schema for creating a new employee."""
    client_employee_id: str = Field(..., min_length=1, max_length=50)


class EmployeeUpdate(BaseModel):
    """Schema for updating an employee (all fields optional)."""
    fullname: Optional[str] = Field(None, min_length=1, max_length=200)
    work_email: Optional[EmailStr] = None
    dob: Optional[date] = None
    years_of_experience: Optional[Decimal] = Field(None, ge=0, le=99)
    job_code: Optional[str] = None
    job_title: Optional[str] = None
    organization_name: Optional[str] = None
    department_name: Optional[str] = None
    manager_employee_id: Optional[str] = None
    job_start_date: Optional[date] = None
    hire_date: Optional[date] = None
    recent_hire_date: Optional[date] = None
    anniversary_date: Optional[date] = None
    scheduled_weekly_hour: Optional[int] = Field(None, ge=0, le=168)
    term_date: Optional[date] = None
    is_active: Optional[bool] = None


class EmployeeResponse(BaseModel):
    """Schema for employee response."""
    client_employee_id: str
    fullname: Optional[str] = None
    work_email: Optional[str] = None
    dob: Optional[date] = None
    years_of_experience: Optional[Decimal] = None
    job_code: Optional[str] = None
    job_title: Optional[str] = None
    organization_name: Optional[str] = None
    department_name: Optional[str] = None
    manager_employee_id: Optional[str] = None
    job_start_date: Optional[date] = None
    hire_date: Optional[date] = None
    recent_hire_date: Optional[date] = None
    anniversary_date: Optional[date] = None
    scheduled_weekly_hour: Optional[int] = None
    term_date: Optional[date] = None
    tenure: Optional[int] = None
    is_active: Optional[bool] = None
    is_rehired: Optional[bool] = None
    is_early_attrition: Optional[bool] = None
    loaded_at: Optional[datetime] = None
    source_file: Optional[str] = None
    
    class Config:
        from_attributes = True


class EmployeeListResponse(BaseModel):
    """Schema for paginated employee list response."""
    total: int
    page: int
    limit: int
    employees: list[EmployeeResponse]
