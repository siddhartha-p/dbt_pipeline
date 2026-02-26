"""
Employee router.
Provides CRUD endpoints for employee management.
"""
from fastapi import APIRouter, Depends, HTTPException, status, Query
from sqlalchemy.orm import Session
from typing import Optional

from api.database import get_db
from api.auth.models import User
from api.dependencies import get_current_user, get_current_admin_user
from api.employees import crud
from api.employees.schemas import (
    EmployeeCreate,
    EmployeeUpdate,
    EmployeeResponse,
    EmployeeListResponse
)

router = APIRouter(prefix="/employees", tags=["Employees"])


@router.get("", response_model=EmployeeListResponse)
def list_employees(
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user),
    page: int = Query(1, ge=1, description="Page number"),
    limit: int = Query(50, ge=1, le=100, description="Items per page"),
    department: Optional[str] = Query(None, description="Filter by department name"),
    is_active: Optional[bool] = Query(None, description="Filter by active status"),
    search: Optional[str] = Query(None, description="Search in name, email, ID, job title")
):
    """
    List all employees with optional filtering and pagination.
    
    - **page**: Page number (default: 1)
    - **limit**: Items per page (default: 50, max: 100)
    - **department**: Filter by department name (partial match)
    - **is_active**: Filter by active status (true/false)
    - **search**: Search in name, email, employee ID, or job title
    """
    skip = (page - 1) * limit
    employees, total = crud.get_employees(
        db,
        skip=skip,
        limit=limit,
        department=department,
        is_active=is_active,
        search=search
    )
    
    return EmployeeListResponse(
        total=total,
        page=page,
        limit=limit,
        employees=employees
    )


@router.get("/{employee_id}", response_model=EmployeeResponse)
def get_employee(
    employee_id: str,
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user)
):
    """
    Get a single employee by ID.
    
    - **employee_id**: The client employee ID
    """
    employee = crud.get_employee(db, employee_id)
    
    if not employee:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Employee with ID '{employee_id}' not found"
        )
    
    return employee


@router.post("", response_model=EmployeeResponse, status_code=status.HTTP_201_CREATED)
def create_employee(
    employee: EmployeeCreate,
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_admin_user)
):
    """
    Create a new employee (Admin only).
    
    - **client_employee_id**: Unique employee identifier
    - **fullname**: Employee's full name
    - **hire_date**: Date the employee was hired
    - Other fields are optional
    """
    # Check if employee ID already exists
    existing = crud.get_employee(db, employee.client_employee_id)
    if existing:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"Employee with ID '{employee.client_employee_id}' already exists"
        )
    
    return crud.create_employee(db, employee)


@router.put("/{employee_id}", response_model=EmployeeResponse)
def update_employee(
    employee_id: str,
    employee_update: EmployeeUpdate,
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_admin_user)
):
    """
    Update an existing employee (Admin only).
    
    - **employee_id**: The client employee ID to update
    - Only provided fields will be updated
    """
    updated = crud.update_employee(db, employee_id, employee_update)
    
    if not updated:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Employee with ID '{employee_id}' not found"
        )
    
    return updated


@router.delete("/{employee_id}", response_model=EmployeeResponse)
def delete_employee(
    employee_id: str,
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_admin_user)
):
    """
    Soft delete an employee (Admin only).
    
    Sets is_active to False and term_date to today.
    
    - **employee_id**: The client employee ID to delete
    """
    deleted = crud.soft_delete_employee(db, employee_id)
    
    if not deleted:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Employee with ID '{employee_id}' not found"
        )
    
    return deleted
