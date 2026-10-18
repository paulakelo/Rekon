# Chaos Scenarios & Expected System Behavior

Real-world payment streams are messy. External payment aggregators retry callbacks when network packets drop, customers pay in installments, bank transfers take hours to clear, and consumers sometimes type wrong account numbers.

Rekon tests and validates 11 explicit chaos scenarios:

---

### Scenario C1: Exact Duplicate Redelivery
- **Description**: Safaricom or bank gateway retries a webhook 3 times with identical receipt ID and payload.
- **Expected Outcome**:
  - `silver_payments` contains exactly 1 row for that `(rail, rail_receipt_id)`.
  - Delta `MERGE` recognizes the existing key and does not insert.
  - Microbatch metrics increment `duplicates_dropped`.

---

### Scenario C2: Duplicate Conflict (Same Receipt, Differing Amount)
- **Description**: An existing receipt ID arrives a second time, but with a different transaction amount (potential spoofing or upstream glitch).
- **Expected Outcome**:
  - The initial record in `silver_payments` is preserved (never overwritten).
  - A row is written to `gold_payment_anomalies` with `anomaly_type = "DUPLICATE_CONFLICT"` and JSON details capturing both amounts.

---

### Scenario C3: Inversion / Pre-Payment (Payment Before Order)
- **Description**: An asynchronous payment confirmation arrives 2 hours before the order creation event reaches Kafka.
- **Expected Outcome**:
  - Payment is safely stored in `silver_payments`.
  - When the order arrives, the order aggregation step matches the existing payment.
  - Order status evaluates to `PAID` without manual intervention.

---

### Scenario C4: Late Payment (Payment After Nominal Window)
- **Description**: Payment arrives after the nominal 48-hour payment window has lapsed.
- **Expected Outcome**:
  - Payment is still accepted and credited to the order in `gold_order_payment_status`.
  - An anomaly is recorded in `gold_payment_anomalies` with `anomaly_type = "LATE_PAYMENT"`.

---

### Scenario C5: Orphan Payment (Unknown Order Reference)
- **Description**: Payer entered a typo in the Paybill account field (`ORD-99999999` which does not exist).
- **Expected Outcome**:
  - Payment is stored in `silver_payments`.
  - When the payment window passes, the sweep job detects no matching order and emits `ORPHAN_PAYMENT` to `gold_payment_anomalies`.

---

### Scenario C6: Split Payment (Multi-Installment Settlement)
- **Description**: An order for 3,000 KES is paid via two separate M-Pesa transactions (1,000 KES + 2,000 KES).
- **Expected Outcome**:
  - First payment sets `status = "PARTIAL"`, `paid_total_kes = 1000`, `payment_count = 1`.
  - Second payment transitions `status = "PAID"`, `paid_total_kes = 3000`, `payment_count = 2`.

---

### Scenario C7: Underpayment (Permanent Partial Payment)
- **Description**: Customer pays 2,500 KES for a 3,000 KES order and abandons the transaction.
- **Expected Outcome**:
  - Stays `PARTIAL` during the active window.
  - After 48 simulated hours, the sweep job marks status as `UNDERPAID` and emits an anomaly.

---

### Scenario C8: Overpayment
- **Description**: Customer mistakenly pays 4,000 KES for a 3,000 KES order.
- **Expected Outcome**:
  - `gold_order_payment_status` sets `status = "OVERPAID"`.
  - Emits `OVERPAID` anomaly in `gold_payment_anomalies` so finance can issue a refund.

---

### Scenario C9: Malformed Payload
- **Description**: Corrupted bytes, truncated JSON, or missing critical fields sent to webhook receiver.
- **Expected Outcome**:
  - Raw body stored in Bronze (no data lost at edge).
  - Silver routes the record to `silver_rejects` with `reject_reason = "MALFORMED_JSON"` or missing field details.

---

### Scenario C10: Worker Crash & Restart Mid-Batch
- **Description**: Spark process receives `SIGKILL` in the middle of a micro-batch.
- **Expected Outcome**:
  - Delta Lake transaction log maintains atomicity.
  - Restart resumes from checkpoint; no duplicate rows written, no uncommitted state.

---

### Scenario C11: Checkpoint Wipe & Topic Replay
- **Description**: Checkpoint directory deleted; topics replayed from offset 0.
- **Expected Outcome**:
  - Delta Lake idempotent MERGE produces bit-for-bit identical Gold tables.
