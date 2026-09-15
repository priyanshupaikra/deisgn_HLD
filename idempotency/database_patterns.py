"""
╔══════════════════════════════════════════════════════════════════════════════╗
║                       IDEMPOTENCY — MODULE 3                                 ║
║                   DATABASE-LEVEL IDEMPOTENCY PATTERNS                        ║
╚══════════════════════════════════════════════════════════════════════════════╝

CONCEPT:
--------
Beyond API-level deduplication, idempotency must be enforced at the DATABASE
layer as well. This provides a "last line of defense" against duplicates
even if the application layer fails.

THREE DATABASE IDEMPOTENCY PATTERNS:
──────────────────────────────────────
  1. UPSERT (INSERT OR UPDATE)
       If row exists → UPDATE. If not → INSERT. Always exactly one row.

  2. UNIQUE CONSTRAINTS
       The database ENFORCES uniqueness at the storage level.
       Duplicate inserts → database raises unique violation → app handles it.

  3. OPTIMISTIC LOCKING (Version Columns / Compare-And-Swap)
       Detect concurrent modifications using a version number.
       Only commit if version hasn't changed (no lost updates).

PATTERN 1: UPSERT
──────────────────
  SQL: INSERT ... ON CONFLICT DO UPDATE ...  (PostgreSQL)
       INSERT ... ON DUPLICATE KEY UPDATE ... (MySQL)
       MERGE INTO ...                         (Oracle, SQL Server)

  Use case:
    Customer profile updates — whether it's a new customer or existing,
    the end result should be a correctly populated row.

  EXAMPLE:
    First call:  UPSERT user(email="a@b.com", name="Alice") → INSERT → 1 row
    Second call: UPSERT user(email="a@b.com", name="Alice") → UPDATE → 1 row
    Result: always exactly 1 row with the correct data.

PATTERN 2: UNIQUE CONSTRAINTS
───────────────────────────────
  Add a UNIQUE INDEX on business keys (not just primary key):
    - email column on users table
    - (user_id, product_id) on order_items table
    - idempotency_key column on payments table

  Flow:
    Client retries → tries to INSERT → DB raises UNIQUE_VIOLATION
    Application catches the error → treats as "already done" → returns cached result

  WHY NOT JUST USE APP-LEVEL CHECKS?
    Check-then-insert has a RACE CONDITION:
      Thread 1: checks → user doesn't exist
      Thread 2: checks → user doesn't exist
      Thread 1: inserts → success
      Thread 2: inserts → DUPLICATE! (both passed the check)

    UNIQUE CONSTRAINT removes this race: DB handles it atomically.

PATTERN 3: OPTIMISTIC LOCKING
───────────────────────────────
  Attach a version number to every row.
  On update: check that version matches what you read, then increment.

  SQL:
    SELECT id, name, version FROM orders WHERE id = 42
    -- Read: version = 3
    UPDATE orders SET status='shipped', version=4 WHERE id=42 AND version=3
    -- If 0 rows affected: someone else updated it first → retry

  This prevents the "lost update" problem:
    Thread A reads order (version=3)
    Thread B reads order (version=3)
    Thread A updates → version=4
    Thread B tries to update WHERE version=3 → NO ROWS AFFECTED → detected!
    Thread B: knows its read is stale → retry with fresh data

"""

import time
import threading
import uuid
from typing import Optional
from dataclasses import dataclass, field
from enum import Enum


# ─────────────────────────────────────────────────────────────────────────────
# Database row models
# ─────────────────────────────────────────────────────────────────────────────

class DuplicateKeyError(Exception):
    """Raised when a UNIQUE constraint is violated (like PostgreSQL UNIQUE_VIOLATION)."""
    pass


class StaleVersionError(Exception):
    """Raised when optimistic lock version mismatch detected."""
    pass


@dataclass
class PaymentRow:
    """Simulates a row in the 'payments' database table."""
    id: str
    idempotency_key: str          # UNIQUE constraint
    user_id: str
    amount: float
    status: str = "pending"
    created_at: float = field(default_factory=time.time)

    def __repr__(self):
        return (f"Payment(id={self.id[:8]}..., amt={self.amount}, "
                f"status={self.status}, idem={self.idempotency_key[:8]}...)")


@dataclass
class OrderRow:
    """Simulates a row in the 'orders' table with optimistic locking."""
    id: str
    user_id: str
    status: str
    total: float
    version: int = 1              # Optimistic lock version
    updated_at: float = field(default_factory=time.time)

    def __repr__(self):
        return (f"Order(id={self.id[:8]}..., status={self.status}, "
                f"total={self.total}, version={self.version})")


@dataclass
class UserProfileRow:
    """Simulates a user profile row for UPSERT demo."""
    email: str                    # UNIQUE — the natural key for upsert
    name: str
    phone: str = ""
    tier: str = "free"
    updated_at: float = field(default_factory=time.time)


# ─────────────────────────────────────────────────────────────────────────────
# PATTERN 1: UPSERT
# ─────────────────────────────────────────────────────────────────────────────

class UpsertPattern:
    """
    INSERT OR UPDATE — ensure exactly one row exists for a natural key.

    Simulates PostgreSQL:
      INSERT INTO user_profiles (email, name, phone, tier)
      VALUES ($1, $2, $3, $4)
      ON CONFLICT (email) DO UPDATE SET
        name = EXCLUDED.name,
        phone = EXCLUDED.phone,
        tier = EXCLUDED.tier,
        updated_at = NOW();

    KEY PROPERTY:
      Calling this N times with the SAME data = exactly ONE row, always.
      No duplicates. No errors. Safe to retry.
    """

    def __init__(self):
        # In-memory table: {email → UserProfileRow}
        self._table: dict[str, UserProfileRow] = {}
        self.insert_count = 0
        self.update_count = 0

    def upsert(self, email: str, name: str, phone: str = "", tier: str = "free") -> tuple[UserProfileRow, bool]:
        """
        INSERT OR UPDATE user_profiles ON CONFLICT (email) DO UPDATE.

        :return: (row, was_inserted) — True if new row, False if updated
        """
        existing = self._table.get(email)

        if existing is None:
            # INSERT path
            row = UserProfileRow(
                email=email, name=name, phone=phone, tier=tier, updated_at=time.time()
            )
            self._table[email] = row
            self.insert_count += 1
            print(f"    [UPSERT] INSERT: {email} → new row created")
            return row, True
        else:
            # UPDATE path (ON CONFLICT DO UPDATE)
            existing.name = name
            existing.phone = phone
            existing.tier = tier
            existing.updated_at = time.time()
            self.update_count += 1
            print(f"    [UPSERT] UPDATE: {email} → existing row updated (no duplicate!)")
            return existing, False

    def find(self, email: str) -> Optional[UserProfileRow]:
        return self._table.get(email)

    def count(self) -> int:
        return len(self._table)


# ─────────────────────────────────────────────────────────────────────────────
# PATTERN 2: UNIQUE CONSTRAINT (Idempotency via natural key)
# ─────────────────────────────────────────────────────────────────────────────

class UniqueConstraintPattern:
    """
    UNIQUE constraint on idempotency_key column in the payments table.

    Simulates:
      CREATE TABLE payments (
        id             UUID PRIMARY KEY,
        idempotency_key UUID UNIQUE NOT NULL,   ← THE KEY CONSTRAINT
        user_id         VARCHAR NOT NULL,
        amount          DECIMAL NOT NULL,
        status          VARCHAR DEFAULT 'pending'
      );

    On duplicate key:
      INSERT INTO payments (..., idempotency_key, ...)
      VALUES (..., 'uuid-abc', ...)
      → ON CONFLICT (idempotency_key): raise UNIQUE_VIOLATION

    Application handles UNIQUE_VIOLATION:
      SELECT * FROM payments WHERE idempotency_key = 'uuid-abc'
      Return the existing payment record (not an error!)

    WHY UNIQUE CONSTRAINT > APP CHECK:
      App check: SELECT → (race condition gap) → INSERT  ← UNSAFE
      DB constraint: INSERT (atomic)  → UNIQUE_VIOLATION  ← SAFE
    """

    def __init__(self):
        self._by_id: dict[str, PaymentRow] = {}
        self._by_idem_key: dict[str, PaymentRow] = {}   # UNIQUE index
        self._db_lock = threading.Lock()  # Simulate DB transaction lock

    def insert_payment(self, idempotency_key: str, user_id: str,
                       amount: float) -> PaymentRow:
        """
        INSERT INTO payments.
        Raises DuplicateKeyError if idempotency_key already exists.

        This simulates what the DB does atomically.
        The application should catch DuplicateKeyError and treat it as
        "already processed" — return the existing row.
        """
        with self._db_lock:
            if idempotency_key in self._by_idem_key:
                # UNIQUE CONSTRAINT VIOLATION
                raise DuplicateKeyError(
                    f"payment with idempotency_key={idempotency_key[:12]}... already exists"
                )

            payment = PaymentRow(
                id=str(uuid.uuid4()),
                idempotency_key=idempotency_key,
                user_id=user_id,
                amount=amount,
                status="completed",
            )
            self._by_id[payment.id] = payment
            self._by_idem_key[idempotency_key] = payment
            return payment

    def find_by_idem_key(self, idempotency_key: str) -> Optional[PaymentRow]:
        return self._by_idem_key.get(idempotency_key)

    def safe_insert_or_get(self, idempotency_key: str,
                            user_id: str, amount: float) -> tuple[PaymentRow, bool]:
        """
        Insert-or-get: idempotent wrapper around insert.
        Returns (payment, was_new).

        CORRECT PATTERN:
          try:
              payment = db.insert_payment(key, ...)
              return payment, True   # Newly created
          except DuplicateKeyError:
              payment = db.find_by_idem_key(key)
              return payment, False  # Already existed
        """
        try:
            payment = self.insert_payment(idempotency_key, user_id, amount)
            print(f"    [UniqueConstraint] INSERT success → payment={payment.id[:8]}...")
            return payment, True
        except DuplicateKeyError:
            payment = self.find_by_idem_key(idempotency_key)
            print(f"    [UniqueConstraint] DUPLICATE KEY → returning existing={payment.id[:8]}...")
            return payment, False


# ─────────────────────────────────────────────────────────────────────────────
# PATTERN 3: OPTIMISTIC LOCKING (Version Columns)
# ─────────────────────────────────────────────────────────────────────────────

class OptimisticLockingPattern:
    """
    Optimistic Locking using a version column.

    Prevents "lost updates" — two concurrent reads both update the same row,
    but only one should win.

    Simulates:
      UPDATE orders
      SET status = 'shipped', version = version + 1
      WHERE id = $1 AND version = $2  ← Only update if version matches!

    If rows_affected == 0:
      → Someone else updated first → version doesn't match → STALE DATA
      → Application must retry with fresh read

    COMPARISON:
      Pessimistic locking: SELECT ... FOR UPDATE (blocks other readers)
      Optimistic locking:  No locks during read, check on write (more scalable)

    WHEN TO USE:
      Optimistic: Low contention (reads >> writes), short transactions
      Pessimistic: High contention (many concurrent writes to same row)
    """

    def __init__(self):
        self._orders: dict[str, OrderRow] = {}
        self._db_lock = threading.Lock()

    def create_order(self, user_id: str, total: float) -> OrderRow:
        order = OrderRow(
            id=str(uuid.uuid4()),
            user_id=user_id,
            status="pending",
            total=total,
            version=1,
        )
        self._orders[order.id] = order
        return order

    def find_order(self, order_id: str) -> Optional[OrderRow]:
        """Read order (no lock in optimistic pattern)."""
        return self._orders.get(order_id)

    def update_status(self, order_id: str, new_status: str,
                      expected_version: int) -> OrderRow:
        """
        UPDATE orders
        SET status = $new_status, version = version + 1
        WHERE id = $order_id AND version = $expected_version

        :raises StaleVersionError: if version doesn't match (lost update detected)
        """
        with self._db_lock:
            order = self._orders.get(order_id)
            if not order:
                raise KeyError(f"Order {order_id} not found")

            # THE CORE OPTIMISTIC LOCK CHECK
            if order.version != expected_version:
                raise StaleVersionError(
                    f"Version mismatch: expected={expected_version}, "
                    f"actual={order.version}. Someone else updated this order!"
                )

            # Versions match → safe to update
            order.status = new_status
            order.version += 1      # Increment version on every successful update
            order.updated_at = time.time()
            print(f"    [OptimisticLock] UPDATE order {order_id[:8]}... "
                  f"→ status={new_status}, version={order.version}")
            return order

    def update_with_retry(self, order_id: str, new_status: str,
                           max_retries: int = 3) -> OrderRow:
        """
        Retry loop for optimistic locking.
        On StaleVersionError: re-read the order and retry.
        """
        for attempt in range(1, max_retries + 1):
            order = self.find_order(order_id)
            if not order:
                raise KeyError(f"Order {order_id} not found")

            try:
                return self.update_status(order_id, new_status, order.version)
            except StaleVersionError as e:
                print(f"    [OptimisticLock] Attempt {attempt} failed: {e}")
                if attempt == max_retries:
                    raise
                time.sleep(0.01)  # Brief back-off before retry


# ─────────────────────────────────────────────────────────────────────────────
# DEMO / DRIVER CODE
# ─────────────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    print("=" * 65)
    print("   DATABASE IDEMPOTENCY PATTERNS — Demo")
    print("=" * 65)

    # ── PATTERN 1: UPSERT ────────────────────────────────────────────────
    print("\n  ═══ PATTERN 1: UPSERT (INSERT OR UPDATE) ═══")
    upsert = UpsertPattern()

    print("\n  Calling upsert 3x with same email (simulating retries):")
    for i in range(3):
        row, is_new = upsert.upsert("alice@ex.com", "Alice", "+1-555-1234", "premium")
        print(f"  Attempt {i+1}: is_new={is_new} | rows_in_table={upsert.count()}")

    print(f"\n  Rows in table: {upsert.count()} (expected: 1 — no duplicates!)")
    print(f"  Inserts: {upsert.insert_count}, Updates: {upsert.update_count}")

    # ── PATTERN 2: UNIQUE CONSTRAINT ──────────────────────────────────────
    print("\n\n  ═══ PATTERN 2: UNIQUE CONSTRAINT ═══")
    uc = UniqueConstraintPattern()
    idem_key = str(uuid.uuid4())

    print(f"\n  idempotency_key={idem_key[:12]}...")
    print("  Simulating 3 retries of the same payment:\n")

    all_results = []
    for i in range(3):
        payment, is_new = uc.safe_insert_or_get(
            idempotency_key=idem_key,
            user_id="user_42",
            amount=99.99,
        )
        all_results.append(payment.id)
        print(f"  Attempt {i+1}: payment_id={payment.id[:8]}... is_new={is_new}")

    unique_ids = set(all_results)
    print(f"\n  All 3 attempts got the SAME payment ID: {len(unique_ids)==1}")
    print(f"  Total payments in DB: {len(uc._by_id)} (expected: 1)")

    # ── PATTERN 3: OPTIMISTIC LOCKING ─────────────────────────────────────
    print("\n\n  ═══ PATTERN 3: OPTIMISTIC LOCKING ═══")
    ol = OptimisticLockingPattern()

    order = ol.create_order("user_42", total=299.99)
    print(f"\n  Created order: {order}")

    # Normal update
    print("\n  Normal update (version matches):")
    updated = ol.update_status(order.id, "confirmed", expected_version=1)
    print(f"  After update: {updated}")

    # Stale version update (simulates concurrent write)
    print("\n  Stale update (version=1 but actual=2 — lost update detected):")
    try:
        ol.update_status(order.id, "shipped", expected_version=1)  # Wrong version!
    except StaleVersionError as e:
        print(f"  StaleVersionError caught: {e}")

    # Retry-enabled update (handles the stale version automatically)
    print("\n  Retry-enabled update (auto-reads fresh version):")
    final = ol.update_with_retry(order.id, "shipped")
    print(f"  Final order: {final}")

    # Simulate two concurrent threads racing
    print("\n  Concurrent race: two threads updating the same order simultaneously:")
    order2 = ol.create_order("user_99", 150.0)
    wins = []
    failures = []

    def concurrent_update(new_status):
        try:
            # Both read version=1 at the same time
            time.sleep(0.01)  # Simulate read delay
            o = ol.find_order(order2.id)
            ol.update_status(order2.id, new_status, expected_version=o.version)
            wins.append(new_status)
        except StaleVersionError:
            failures.append(new_status)

    t1 = threading.Thread(target=concurrent_update, args=("processing",))
    t2 = threading.Thread(target=concurrent_update, args=("cancelled",))
    t1.start(); t2.start()
    t1.join(); t2.join()

    print(f"\n  Winner: {wins} | Lost (stale): {failures}")
    print(f"  Final order status: {ol.find_order(order2.id).status} "
          f"(exactly ONE winner, no silent overwrite)")

    print("\n" + "=" * 65)
    print("  DB IDEMPOTENCY PATTERNS COMPARISON")
    print("=" * 65)
    rows = [
        ("Pattern",           "Mechanism",               "Best For"),
        ("─"*16,              "─"*25,                    "─"*20),
        ("UPSERT",            "ON CONFLICT DO UPDATE",   "Profile sync, config updates"),
        ("Unique Constraint",  "DB rejects duplicates",  "Payments, deduplication"),
        ("Optimistic Lock",    "Version column check",    "Concurrent modifications"),
    ]
    for r in rows:
        print(f"  {r[0]:<18} {r[1]:<27} {r[2]}")
    print("=" * 65)
