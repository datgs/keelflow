"""Seed + apply schema for the rag_demo.

    uv run --package keelflow-demo-rag python -m rag_demo.seed
    uv run --package keelflow-demo-rag python -m rag_demo.seed --reset

Employee-helpdesk knowledge base — synthetic docs only.

`--reset` clears ONLY the demo-owned RAG tables (docs/chunks/questions/
settings) before seeding — use it when switching fixture content or
starting a clean demo. Plain `seed` appends/upserts and keeps history.
"""
from __future__ import annotations

import os
import sys
from pathlib import Path

import psycopg

DSN = os.environ.get("KEELFLOW_DSN",
                     "postgresql://postgres:postgres@localhost:5544/keelflow")

DOCS = [
    ("Password reset",
     ("To reset your password, open the company portal and click "
      "Forgot Password. A reset link is emailed within one minute and "
      "expires in 15 minutes. If the link does not arrive, check the "
      "spam folder, then call IT support at extension 4357. Managers "
      "cannot reset passwords for their team.")),
    ("VPN setup",
     ("Install the SecureConnect client from the software center. Sign "
      "in with your company email and approve the push notification on "
      "your phone. VPN is required for the internal wiki, finance "
      "tools, and file shares. The client reconnects automatically "
      "when the network changes.")),
    ("Requesting a laptop",
     ("New laptops are approved every two years. File a hardware "
      "request in the IT portal with your manager's approval. "
      "Standard models ship in three business days; high-spec models "
      "need a written justification and take two weeks. Broken "
      "laptops are replaced within one business day.")),
    ("Annual leave policy",
     ("Full-time employees get 18 days of paid annual leave per year, "
      "accrued monthly. Requests under three days need manager "
      "approval in the HR portal; longer leave needs a director "
      "signoff. Up to five unused days carry over into January of "
      "the next year.")),
    ("Sick leave",
     ("Sick leave does not count against annual leave. Notify your "
      "manager before 9am on the first day. A doctor's note is "
      "required after three consecutive days. Part-day absences are "
      "recorded in the HR portal under half-day sick.")),
    ("Working from home",
     ("Employees may work from home two days per week with manager "
      "agreement. Fully remote contracts require director approval. "
      "Home office equipment (monitor, chair) can be reimbursed up "
      "to $300 once per year with a receipt.")),
    ("Health insurance",
     ("The company covers a premium health plan for employees and a "
      "50% subsidy for dependants. Enrollment happens in the HR "
      "portal within 30 days of joining or during November open "
      "enrollment. Dental and vision are optional add-ons.")),
    ("Payroll dates",
     ("Salaries are paid on the 28th of each month, or the last "
      "working day before it. Payslips appear in the HR portal on "
      "payday. Bonus payouts follow the March review cycle. Bank "
      "account changes must be submitted before the 15th to take "
      "effect the same month.")),
    ("Expense claims",
     ("Submit expense claims in the finance portal with a photo of "
      "each receipt. Meals while travelling are reimbursed up to "
      "$60 per day; taxis up to $40 per trip. Claims under $500 are "
      "auto-approved and paid in the next payroll run. Claims above "
      "$500 need manager signoff.")),
    ("Office hours and access",
     ("The office is open from 7am to 9pm on weekdays. Your badge "
      "opens the main entrance and your team's floor. Visitor badges "
      "are collected at reception and must be registered in advance "
      "by the host. Lost badges are replaced by security for a $10 "
      "fee.")),
    ("Cafeteria",
     ("The cafeteria on floor 2 serves breakfast until 9:30 and lunch "
      "from 11:30 to 1:30. A subsidized meal costs $4. Vegetarian and "
      "halal options are available daily. Coffee and tea on every "
      "floor are free. Allergen lists are posted at each counter.")),
    ("Parking and commute",
     ("Parking passes are requested in the facilities portal; "
      "allocation is monthly. The company subsidizes 80% of public "
      "transit passes — submit the receipt as a commuter expense. "
      "Bike parking is free in the basement with a showers room on "
      "level B1.")),
    ("IT security basics",
     ("Use the password manager for all work accounts — never reuse "
      "passwords. Multi-factor approval is required for finance and "
      "admin tools. Report suspicious emails with the Report Phish "
      "button. USB storage devices are blocked by default; request an "
      "exception from IT security.")),
    ("Onboarding — first week",
     ("Your laptop, badge, and accounts are ready on day one. Complete "
      "the mandatory security and privacy training in the first week. "
      "Your buddy introduces the team rituals: standup at 9:15, "
      "planning on Monday, demo on Friday. Probation reviews happen "
      "at 30, 60, and 90 days.")),
    ("Meeting rooms",
     ("Book meeting rooms in the calendar tool — rooms auto-release "
      "after 10 minutes if nobody checks in at the door tablet. "
      "Video calls use the built-in camera bar; press the green "
      "button to join. Large rooms above eight seats need a day's "
      "notice.")),
    ("Sample question bank",
     ("Typical assistant questions: How do I reset my password? "
      "How many days of annual leave do I get? What is the daily "
      "meal limit on a business trip? When is payday? Can I work "
      "from home? How do I book a meeting room?")),
]


def main() -> None:
    reset = "--reset" in sys.argv
    schema = Path(__file__).with_name("schema.sql").read_text()
    with psycopg.connect(DSN, autocommit=True) as conn:
        conn.execute(schema)
        if reset:
            conn.execute(
                "TRUNCATE demo_rag_docs, demo_rag_chunks,"
                " demo_rag_questions, demo_rag_settings")
        for title, body in DOCS:
            conn.execute(
                "INSERT INTO demo_rag_docs (title, body) VALUES (%s, %s)",
                (title, body))
        conn.execute(
            "INSERT INTO demo_rag_settings (key, value) VALUES"
            " ('slow_llm','false'), ('flaky_embed','false'),"
            " ('weak_retrieval','false')"
            " ON CONFLICT (key) DO NOTHING")
        n = conn.execute("SELECT count(*) FROM demo_rag_docs").fetchone()[0]
        print(f"seeded {n} docs")


if __name__ == "__main__":
    main()
