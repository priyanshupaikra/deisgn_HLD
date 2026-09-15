"""
╔══════════════════════════════════════════════════════════════════════════════╗
║                AUTHENTICATION & AUTHORIZATION — MODULE 2                     ║
║                     AUTHORIZATION: RBAC VS ABAC                              ║
╚══════════════════════════════════════════════════════════════════════════════╝

WHAT IS AUTHORIZATION?
──────────────────────
Authentication is "Who are you?".
Authorization (AuthZ) is "Are you allowed to do this?"

1. ROLE-BASED ACCESS CONTROL (RBAC):
   - The most common system. 
   - Users are assigned Roles (e.g., Admin, Editor, Viewer).
   - Roles are granted Permissions (e.g., Editor can `edit_post`).
   - Pros: Easy to understand and implement.
   - Cons: Role explosion. (What if you need a "Billing Admin" who can't edit posts?
           What if you need a "Junior Editor" who can only edit in the 'Sports' category?)

2. ATTRIBUTE-BASED ACCESS CONTROL (ABAC):
   - Fine-grained permissions based on a combination of Attributes.
   - Evaluates:
     - User Attributes (e.g., department='finance', clearance_level=3)
     - Resource Attributes (e.g., document_type='invoice', status='draft')
     - Environment Attributes (e.g., time_of_day, ip_address)
   - Rule Example: "Users in department 'finance' can 'read' documents of type 
     'invoice' ONLY IF they are on the corporate IP address."
   - Pros: Infinitely flexible and fine-grained.
   - Cons: Very complex to build and audit.
"""

from dataclasses import dataclass
from typing import List


# ─────────────────────────────────────────────────────────────────────────────
# 1. ROLE-BASED ACCESS CONTROL (RBAC)
# ─────────────────────────────────────────────────────────────────────────────

class RBACSystem:
    def __init__(self):
        # Role -> List of Permissions
        self.role_permissions = {
            "admin": ["read_post", "write_post", "delete_post", "manage_users"],
            "editor": ["read_post", "write_post"],
            "viewer": ["read_post"]
        }
        
        # User -> Role
        self.user_roles = {
            "alice": "admin",
            "bob": "editor",
            "charlie": "viewer"
        }

    def check_permission(self, username: str, permission: str) -> bool:
        role = self.user_roles.get(username)
        if not role:
            return False
            
        allowed_permissions = self.role_permissions.get(role, [])
        if permission in allowed_permissions:
            print(f"  [RBAC] ✅ '{username}' (Role: {role}) is allowed to '{permission}'")
            return True
        else:
            print(f"  [RBAC] ❌ '{username}' (Role: {role}) denied access to '{permission}'")
            return False


# ─────────────────────────────────────────────────────────────────────────────
# 2. ATTRIBUTE-BASED ACCESS CONTROL (ABAC)
# ─────────────────────────────────────────────────────────────────────────────

@dataclass
class User:
    username: str
    department: str
    is_remote_worker: bool

@dataclass
class Document:
    doc_id: str
    doc_type: str
    classification: str # 'public', 'internal', 'confidential'

@dataclass
class Context:
    time_of_day: int # 0-23
    ip_address: str


class ABACSystem:
    def __init__(self):
        # In a real system, these rules are stored in a Policy Engine (like OPA - Open Policy Agent)
        pass

    def evaluate_policy(self, user: User, action: str, resource: Document, context: Context) -> bool:
        print(f"  [ABAC] Evaluating: Can {user.username} {action} {resource.doc_id}?")
        
        # Policy 1: Anyone can read public documents
        if action == "read" and resource.classification == "public":
            print("    -> Matched Policy 1 (Public Document). Allowed.")
            return True
            
        # Policy 2: Only Finance can access 'invoice' documents
        if resource.doc_type == "invoice":
            if user.department != "finance":
                print("    -> Failed Policy 2 (Not in Finance). Denied.")
                return False
                
        # Policy 3: Confidential documents CANNOT be accessed by remote workers or outside 9-5
        if resource.classification == "confidential":
            if user.is_remote_worker:
                print("    -> Failed Policy 3 (Remote Worker access to Confidential). Denied.")
                return False
            if not (9 <= context.time_of_day <= 17):
                print("    -> Failed Policy 3 (Outside office hours). Denied.")
                return False

        print("    -> All checks passed for this specific context. Allowed.")
        return True


# ─────────────────────────────────────────────────────────────────────────────
# DEMO
# ─────────────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    print("=" * 65)
    print("   AUTHORIZATION: RBAC vs ABAC — Demo")
    print("=" * 65)
    
    print("\n  --- 1. RBAC (Role-Based) ---")
    rbac = RBACSystem()
    rbac.check_permission("alice", "delete_post") # Admin
    rbac.check_permission("bob", "delete_post")   # Editor
    rbac.check_permission("bob", "write_post")    # Editor


    print("\n  --- 2. ABAC (Attribute-Based) ---")
    abac = ABACSystem()
    
    # Users
    dave = User("dave", department="engineering", is_remote_worker=False)
    eve = User("eve", department="finance", is_remote_worker=True)
    frank = User("frank", department="finance", is_remote_worker=False)
    
    # Documents
    q1_earnings = Document("doc_1", doc_type="invoice", classification="confidential")
    lunch_menu = Document("doc_2", doc_type="menu", classification="public")
    
    # Contexts
    office_hours = Context(time_of_day=14, ip_address="192.168.1.1")
    midnight = Context(time_of_day=2, ip_address="192.168.1.1")

    # Tests
    abac.evaluate_policy(dave, "read", lunch_menu, office_hours)
    print("")
    # Dave is engineering, not finance. Should fail.
    abac.evaluate_policy(dave, "read", q1_earnings, office_hours) 
    print("")
    # Eve is finance, but remote. Should fail confidential check.
    abac.evaluate_policy(eve, "read", q1_earnings, office_hours)
    print("")
    # Frank is finance, in office, during office hours. Should succeed.
    abac.evaluate_policy(frank, "read", q1_earnings, office_hours)
    print("")
    # Frank tries at 2 AM. Should fail.
    abac.evaluate_policy(frank, "read", q1_earnings, midnight)
