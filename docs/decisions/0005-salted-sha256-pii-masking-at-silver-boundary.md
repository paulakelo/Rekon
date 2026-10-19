# ADR 0005: Salted SHA-256 PII Masking at the Silver Boundary

## Context
Payment streams inherently contain sensitive Personally Identifiable Information (PII), such as customer mobile telephone numbers (MSISDN `2547XXXXXXXX`) on M-Pesa and bank account numbers on bank settlements. Under privacy regulations (e.g. Kenya Data Protection Act 2019, GDPR), storing raw PII in analytics-accessible tables creates legal exposure and privacy risks.

However, downstream reconciliation and customer analytics still require the ability to group multiple payments made by the same customer (e.g., in split installment payments).

## Decision
We enforce strict PII sanitation at the Silver ingestion boundary:
1. Raw PII is preserved strictly in append-only Bronze Delta tables (accessible only by root audit / replay services).
2. Before inserting into `silver_payments`, the payer identifier is hashed using SHA-256 with an environment-injected secret salt:
   ```python
   hashlib.sha256(f"{cleaned_identifier}:{salt}".encode()).hexdigest()[:24]
   ```
3. Only the resulting 24-character hexadecimal `payer_hash` is written to `silver_payments`. The raw `MSISDN` and account number columns are excluded from the Silver schema.

## Consequences
- **Positive**: Compliant with data protection regulations; data analysts and BI queries cannot view raw customer phone numbers or bank accounts.
- **Positive**: Deterministic hashing preserves identity grouping, enabling split payment analysis and customer lifetime value computation.
- **Trade-off**: If the secret salt is lost or rotated without re-hashing historical Bronze data, cross-time identity linking breaks.
