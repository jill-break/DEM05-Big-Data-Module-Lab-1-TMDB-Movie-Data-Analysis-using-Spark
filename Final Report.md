# Final Report: TMDB Movie Data Analysis Pipeline
## Technology Stack: 
Python 3.11, Apache Spark (PySpark 3.5), TMDb API, Matplotlib/Seaborn (Visualization), Pytest

---
### 1. Executive Summary
This project successfully established a robust, scalable ETL (Extract, Transform, Load) pipeline to analyze high-profile movie performance data from The Movie Database (TMDb). Moving beyond traditional single-node processing, this solution leverages Apache Spark to simulate a big data environment capable of handling massive datasets.

**Key Highlights:**

`Pipeline Maturity`: The system transitioned from a basic script to a production-grade application featuring automated retries, structured logging, schema enforcement, and unit testing.

`Data Integrity`: Complex semi-structured data (nested JSON arrays) was successfully parsed without data loss, a common pitfall in standard flat-file processing.

`Business Insight`: The analysis revealed that while Franchise films command higher budgets and revenues, they do not consistently outperform standalone films in terms of User Ratings, suggesting a divergence between commercial success and critical reception.

---

### 2. Technical Architecture & Methodology
The project adopted a modular "Lakehouse" style approach, separating raw data ingestion from processing and analysis.

**2.1 Ingestion Layer (Extract)**
Objective: Securely fetch data from an external REST API while adhering to rate limits.

*Implementation:*

`Resilience:` Implemented a custom `fetch_single_movie` function with exponential backoff. It automatically detects 429 Too Many Requests errors and waits before retrying, ensuring the pipeline never crashes due to API throttling.

`Observability:` Integrated a file-based logging system (logs/ingestion.log). This provides an audit trail of every API attempt, success, or failure, which is critical for debugging in production environments.

**2.2 Processing & Analysis Layer (Transform)**
`Objective:` Clean, structure, and analyze large-scale movie data.

`Engine:` **Apache Spark (PySpark)** was used exclusively for both ETL transformations and complex KPI calculations. This replaces traditional "hybrid" workflows (using Pandas for aggregation) to ensure true horizontal scalability.

**Key Engineering Challenge:**

`The Problem:` The raw data contained nested arrays (e.g., genres, production_companies) that Spark initially misidentified as "Maps" or "Strings," leading to potential data loss.

`The Solution:` I defined explicit StructType Schemas in src/config.py. This forced Spark to recognize the deep structure of the data, allowing us to accurately extract specific fields (like genre.name or collection.name) using vectorized expressions.

`Optimization:`
Used Lazy Evaluation and caching (df.cache()) to optimize memory usage during repeated aggregation steps.

Employed Vectorized Operations (e.g., F.expr("transform(...)")) instead of Python loops, significantly improving processing speed.

**2.3 Quality Assurance (Testing)**
Framework: `pytest`

`Coverage:` A dedicated test suite (tests/test_cleaning.py) was created to validate the business logic.

`Mocking:` I simulated "dirty" data (e.g., unreleased movies, zero-budget films) to prove that the cleaning functions correctly filter and calculate metrics like Profit and ROI before deploying the code.

---

### 3. Data Analysis & Insights
**3.1 The "Blockbuster" Economy**
The dataset confirms the industry's reliance on massive tentpole productions.

`Revenue Drivers:` The top 10% of movies by budget accounted for a disproportionately large share of the total revenue.

`Budget Inflation`: A clear trend line shows the average budget for high-profile movies has steadily increased over the last two decades.

`The ROI Ceiling`: Interestingly, the movies with the highest absolute revenue did not always hold the highest ROI. Moderate-budget hits often yielded higher percentage returns (multipliers) than expensive blockbusters, which require massive grosses just to break even.

**3.2 Franchise vs. Standalone Performance**
A comparative analysis (visualized in the 4-grid report) yielded distinct profiles for these two categories:

`Franchise Films:`

`Risk/Reward:` Characterized by significantly higher average budgets and revenues. The "floor" for revenue is higher, making them a safer bet for studios.

`Stability:` Showed less variance in box office performance compared to standalone films.

`Standalone Films:`

`Critical Reception:` Often rivaled or exceeded franchises in User Ratings, indicating that audiences appreciate original storytelling even if they pay less to see it.

`Volatility:` Showed a wider spread in ROI, containing both massive breakouts and significant flops.

**3.3 Genre Performance**
`Action & Adventure:` These genres dominate the "High Revenue / High Budget" quadrant. They are the engine of the modern box office but come with significant financial risk.

`Science Fiction:` Showed a strong correlation with high ROI, suggesting a passionate fanbase willing to support these films relative to their costs.

---

### 4. Challenges & Solutions
| Challenge                 | Solution |
|---------------------------|----------|
| Schema Inference Failure  | Spark failed to read nested JSON correctly, treating arrays as strings. I implemented a strict custom schema using `StructType` and `ArrayType` to map the data 1:1. |
| API Rate Limiting         | The API would occasionally block requests. I built a retry loop with exponential backoff to handle 429 and 5xx errors gracefully. |
| PySpark on Windows        | Spark struggled to launch Python workers due to environment path issues. I added a utility function to force `PYSPARK_PYTHON` to point to the active virtual environment. |

---

### 5. Conclusion & Future Work

The "TMDB Movie Data Analysis" project demonstrates a complete lifecycle for modern data engineering. By prioritizing scalability (Spark), reliability (Retries/Logging), and maintainability (Modules/Tests), the system is production-ready.

**4.1 What I Learnt**
Through the execution of this lab, I gained practical experience in several advanced engineering concepts:

**Scalable Data Processing (Spark vs. Pandas):** I learned how to shift from "in-memory" processing with Pandas to distributed processing with PySpark. I understood the importance of Lazy Evaluation (transformations are not executed until an action is called) and how to use Vectorized Expressions (e.g., F.expr) to process data efficiently without using slow Python loops.

**Handling Semi-Structured Data:** One of the biggest challenges was parsing nested JSON arrays from the API. I learnt that Spark's automatic schema inference can be unreliable (misidentifying arrays as strings). I mastered the concept of Strict Schema Enforcement, defining custom StructType schemas to ensure no data was lost during ingestion.

**Production-Grade Reliability:** I moved beyond simple scripts by implementing Robust Ingestion Logic. I learnt how to handle real-world API issues like Rate Limits (HTTP 429) and Server Errors (HTTP 500) using exponential backoff and retry loops, ensuring the pipeline is resilient.

**Software Engineering Best Practices:** I learnt to structure a data project professionally by decoupling code into modules (src/) rather than keeping everything in a notebook. I also gained experience in Unit Testing with pytest to validate business logic before deployment and managing sensitive credentials securely using .env files.

**Future Enhancements:**

`Cloud Deployment`: Migrate the pipeline to AWS (EMR or Glue) to run on a real cluster.

`Orchestration:` Wrap the scripts in Apache Airflow to schedule daily data fetching.

`Data Lake:` Store the raw JSON in an S3 bucket (Bronze Layer) and the processed Parquet files in a Silver Layer for a true Lakehouse architecture.

---

### 6. Project Deliverables Checklist
- [x] **Source Code:** Modularized Python scripts (`src/`) for Ingestion, Cleaning, and Analysis.
- [x] **Orchestration:** Jupyter Notebook (`spark_lab.ipynb`) executing the full pipeline.
- [x] **Data Quality:** Unit tests (`tests/`) and Logging (`logs/`).
- [x] **Documentation:** `README.md` with setup instructions.
- [x] **Datasets:** Raw JSON and Processed CSV exports.
