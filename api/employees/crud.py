"""
CRUD operations for Employee model.
"""
from sqlalchemy.orm import Session
from sqlalchemy import func
from typing import Optional
from datetime import datetime, date

from api.employees.models import Employee
from api.employees.schemas import EmployeeCreate, EmployeeUpdate


def get_employee(db: Session, employee_id: str) -> Optional[Employee]:
    """Get a single employee by ID."""
    return db.query(Employee).filter(
        Employee.client_employee_id == employee_id
    ).first()


def get_employees(
    db: Session,
    skip: int = 0,
    limit: int = 100,
    department: Optional[str] = None,
    is_active: Optional[bool] = None,
    search: Optional[str] = None
) -> tuple[list[Employee], int]:
    """
    Get list of employees with optional filtering.
    
    Returns:
        Tuple of (employees list, total count)
    """
    query = db.query(Employee)
    
    # Apply filters
    if department:
        query = query.filter(Employee.department_name.ilike(f"%{department}%"))
    
    if is_active is not None:
        query = query.filter(Employee.is_active == is_active)
    
    if search:
        search_filter = f"%{search}%"
        query = query.filter(
            (Employee.fullname.ilike(search_filter)) |
            (Employee.work_email.ilike(search_filter)) |
            (Employee.client_employee_id.ilike(search_filter)) |
            (Employee.job_title.ilike(search_filter))
        )
    
    # Get total count before pagination
    total = query.count()
    
    # Apply pagination
    employees = query.offset(skip).limit(limit).all()
    
    return employees, total


def create_employee(db: Session, employee: EmployeeCreate) -> Employee:
    """Create a new employee."""
    # Calculate derived fields
    recent_hire = employee.recent_hire_date or employee.hire_date
    
    # Calculate tenure (days since hire / 365)
    tenure = (date.today() - recent_hire).days // 365 if recent_hire else 0
    
    # Determine if rehired
    is_rehired = employee.hire_date != recent_hire if recent_hire else False
    
    db_employee = Employee(
        client_employee_id=employee.client_employee_id,
        fullname=employee.fullname,
        work_email=employee.work_email,
        dob=employee.dob,
        years_of_experience=employee.years_of_experience,
        job_code=employee.job_code,
        job_title=employee.job_title,
        organization_name=employee.organization_name,
        department_name=employee.department_name,
        manager_employee_id=employee.manager_employee_id,
        job_start_date=employee.job_start_date,
        hire_date=employee.hire_date,
        recent_hire_date=recent_hire,
        anniversary_date=employee.anniversary_date,
        scheduled_weekly_hour=employee.scheduled_weekly_hour,
        term_date=None,
        tenure=tenure,
        is_active=True,
        is_rehired=is_rehired,
        is_early_attrition=False,
        loaded_at=datetime.now(),
        source_file="api"
    )
    
    db.add(db_employee)
    db.commit()
    db.refresh(db_employee)
    
    return db_employee


def update_employee(
    db: Session,
    employee_id: str,
    employee_update: EmployeeUpdate
) -> Optional[Employee]:
    """Update an existing employee."""
    db_employee = get_employee(db, employee_id)
    
    if not db_employee:
        return None
    
    # Update only provided fields
    update_data = employee_update.model_dump(exclude_unset=True)
    
    for field, value in update_data.items():
        setattr(db_employee, field, value)
    
    # Recalculate derived fields if relevant fields changed
    if 'term_date' in update_data or 'recent_hire_date' in update_data:
        recent_hire = db_employee.recent_hire_date or db_employee.hire_date
        term = db_employee.term_date
        
        if term and recent_hire:
            db_employee.tenure = (term - recent_hire).days // 365
            db_employee.is_early_attrition = (term - recent_hire).days < 180
        elif recent_hire:
            db_employee.tenure = (date.today() - recent_hire).days // 365
    
    if 'hire_date' in update_data or 'recent_hire_date' in update_data:
        db_employee.is_rehired = (
            db_employee.hire_date != db_employee.recent_hire_date
            if db_employee.recent_hire_date else False
        )
    
    # Update timestamp
    db_employee.loaded_at = datetime.now()
    
    db.commit()
    db.refresh(db_employee)
    
    return db_employee


def soft_delete_employee(db: Session, employee_id: str) -> Optional[Employee]:
    """
    Soft delete an employee by setting is_active to False.
    Sets term_date to today if not already set.
    """
    db_employee = get_employee(db, employee_id)
    
    if not db_employee:
        return None
    
    db_employee.is_active = False
    
    if not db_employee.term_date:
        db_employee.term_date = date.today()
        
        # Recalculate early attrition
        if db_employee.recent_hire_date:
            days_employed = (db_employee.term_date - db_employee.recent_hire_date).days
            db_employee.is_early_attrition = days_employed < 180
            db_employee.tenure = days_employed // 365
    
    db_employee.loaded_at = datetime.now()
    
    db.commit()
    db.refresh(db_employee)
    
    return db_employee
