# Telecom Churn ETL Pipeline

Ένα ολοκληρωμένο Data Engineering project που υλοποιεί μια ροή ETL (Extract, Transform, Load) για δεδομένα τηλεπικοινωνιών, αξιοποιώντας τη Medallion Architecture (από το επίπεδο Bronze στο Silver). Η ενορχήστρωση γίνεται μέσω του Apache Airflow, με εξαγωγή δεδομένων από object storage (MinIO) και φόρτωση σε σχεσιακή βάση (Microsoft SQL Server).

## 🛠️ Τεχνολογίες & Εργαλεία (Tech Stack)

* **Orchestration:** Apache Airflow (Dockerized)

* **Source Storage (Bronze):** MinIO (S3-compatible Object Storage)

* **Data Warehouse (Silver):** Microsoft SQL Server (SSMS)

* **Processing & Transformations:** Python (pandas, SQLAlchemy, PyMSSQL)

## 🏗️ Αρχιτεκτονική της Ροής (DAG Workflow)

Το pipeline εκτελείται σε μηνιαία βάση (μηνιαίο schedule interval) και αποτελείται από τρία κύρια tasks:

1. **`extract_data`:**

   * Δημιουργεί σύνδεση με το MinIO χρησιμοποιώντας το `S3Hook`.

   * Διαβάζει το ακατέργαστο αρχείο `telecom_churn.csv` (Bronze Layer) απευθείας στη μνήμη.

   * Εφαρμόζει αρχικό φιλτράρισμα ποιότητας δεδομένων (αφαίρεση κενών στους κωδικούς, casting σε integers και αφαίρεση διπλοτύπων μέσω `drop_duplicates`).

   * Φιλτράρει το dataset κρατώντας αυστηρά τα δεδομένα του τρέχοντος μήνα (βασισμένο στο `logical_date` / `execution_date` του Airflow).

2. **`clean_data`:**

   * Καθαρίζει και τυποποιεί τις στήλες (π.χ. ομογενοποίηση του πεδίου gender).

   * Προετοιμάζει το τελικό DataFrame για την εισαγωγή στη βάση.

3. **`load_data_ssms`:**

   * **Idempotency Guarantee:** Πριν την εισαγωγή, εκτελεί ένα `DELETE` statement στον SQL Server (`autocommit=True`) για τον συγκεκριμένο μήνα αναφοράς, αποτρέποντας τη δημιουργία διπλοτύπων σε περίπτωση επανεκτέλεσης του DAG.

   * Φορτώνει μαζικά τα καθαρισμένα δεδομένα στον πίνακα `silver.telecom_churn` χρησιμοποιώντας τη μέθοδο `to_sql` του pandas και τον SQLAlchemy engine.

## 💡 Βασικά Χαρακτηριστικά (Key Engineering Concepts)

* **Idempotent Pipeline:** Η ροή είναι σχεδιασμένη ώστε να μπορεί να τρέξει πολλαπλές φορές για τον ίδιο μήνα χωρίς να παραβιάζει την ακεραιότητα των δεδομένων (Primary Key constraints).

* **Data Quality Management:** Αυτόματος εντοπισμός και αφαίρεση διπλότυπων `customer_id` στο επίπεδο της Python, πριν προκληθούν σφάλματα `IntegrityError` στη βάση δεδομένων.

* **In-Memory Processing:** Απευθείας ανάγνωση δεδομένων από το MinIO μέσω `io.StringIO`, χωρίς τη δημιουργία ενδιάμεσων τοπικών αρχείων.

## 🚀 Οδηγίες Εγκατάστασης & Εκτέλεσης

**1. Κλωνοποίηση του αποθετηρίου:**

```
git clone https://github.com/bremerus/airflow_ETL_telecom.git
cd airflow_ETL_telecom

```

**2. Εκκίνηση Υπηρεσιών:**
Βεβαιωθείτε ότι το Docker τρέχει στο σύστημά σας και εκτελέστε:

```
docker-compose up -d

```

**3. Ρυθμίσεις Airflow (Connections):**
Από το UI του Airflow (συνήθως `http://localhost:8080`), ρυθμίστε τα εξής connections:

* `minio_conn`: Τύπος `Amazon S3` (προσθήκη credentials για το MinIO bucket).

* `my_mssql_connection`: Τύπος `Microsoft SQL Server` (Host: `host.docker.internal`, Port: `1433`, και τα αντίστοιχα credentials της βάσης).

**4. Εκτέλεση του DAG:**
Ενεργοποιήστε το DAG `dag_etl_v...` από το UI και παρακολουθήστε την πορεία των tasks μέσα από την προβολή Graph ή Grid.
