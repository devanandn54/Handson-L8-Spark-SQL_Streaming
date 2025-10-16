# Ride Sharing Analytics Using Spark Streaming and Spark SQL.
---
## **Prerequisites**
Before starting the assignment, ensure you have the following software installed and properly configured on your machine:
1. **Python 3.x**:
   - [Download and Install Python](https://www.python.org/downloads/)
   - Verify installation:
     ```bash
     python3 --version
     ```

2. **PySpark**:
   - Install using `pip`:
     ```bash
     pip install pyspark
     ```

3. **Faker**:
   - Install using `pip`:
     ```bash
     pip install faker
     ```

---

## **Setup Instructions**

### **1. Project Structure**

Ensure your project directory follows the structure below:

```
ride-sharing-analytics/
├── outputs/
│   ├── task_1
│   |    └── CSV files of task 1.
|   ├── task_2
│   |    └── CSV files of task 2.
|   └── task_3
│       └── CSV files of task 3.
├── task1.py
├── task2.py
├── task3.py
├── data_generator.py
└── README.md
```

- **data_generator.py/**: generates a constant stream of input data of the schema (trip_id, driver_id, distance_km, fare_amount, timestamp)  
- **outputs/**: CSV files of processed data of each task stored in respective folders.
- **README.md**: Assignment instructions and guidelines.
  
---

### **2. Running the Analysis Tasks**

You can run the analysis tasks either locally.

1. **Execute Each Task **: The data_generator.py should be continuosly running on a terminal. open a new terminal to execute each of the tasks.
   ```bash
     python data_generator.py
     python task1.py
     python task2.py
     python task3.py
   ```

2. **Verify the Outputs**:
   Check the `outputs/` directory for the resulting files:
   ```bash
   ls outputs/
   ```

---

## **Overview**

In this assignment, we will build a real-time analytics pipeline for a ride-sharing platform using Apache Spark Structured Streaming. we will process streaming data, perform real-time aggregations, and analyze trends over time.

## **Objectives**

By the end of this assignment, you should be able to:

1. Task 1: Ingest and parse real-time ride data.
2. Task 2: Perform real-time aggregations on driver earnings and trip distances.
3. Task 3: Analyze trends over time using a sliding time window.

---

## **Task 1: Basic Streaming Ingestion and Parsing**

1. Ingest streaming data from the provided socket (e.g., localhost:9999) using Spark Structured Streaming.
2. Parse the incoming JSON messages into a Spark DataFrame with proper columns (trip_id, driver_id, distance_km, fare_amount, timestamp).

## **Instructions:**
1. Create a Spark session.
2. Use spark.readStream.format("socket") to read from localhost:9999.
3. Parse the JSON payload into columns.
4. Print the parsed data to the console (using .writeStream.format("console")).
The `task1.py` script ingests streaming data from a socket (`localhost:9999`). It parses the incoming JSON messages into a Spark DataFrame with the columns: `trip_id`, `driver_id`, `distance_km`, `fare_amount`, and `timestamp`. The parsed data is then printed to the console.

#### **Sample Output (`outputs/task_1/`)**

The script also writes the raw, parsed stream data to CSV files in the `outputs/task_1` directory. Each row represents a single ride event.

| trip_id                                | driver_id | distance_km | fare_amount | timestamp           |
| -------------------------------------- | --------- | ----------- | ----------- | ------------------- |
| 5dd8d7e6-7ee6-4a4d-b20a-702ea1dcf078     | 59        | 22.65       | 62.30       | 2025-10-15 22:33:12 |
| ...                                    | ...       | ...         | ...         | ...                 |

<br/>

---

## **Task 2: Real-Time Aggregations (Driver-Level)**

1. Aggregate the data in real time to answer the following questions:
  • Total fare amount grouped by driver_id.
  • Average distance (distance_km) grouped by driver_id.
2. Output these aggregations to the console in real time.

## **Instructions:**
1. Reuse the parsed DataFrame from Task 1.
2. Group by driver_id and compute:
3. SUM(fare_amount) as total_fare
4. AVG(distance_km) as avg_distance
5. Store the result in csv

The `task2.py` script aggregates the streaming data in real time to calculate the cumulative total fare and average trip distance for each driver.

#### **Sample Output (`outputs/task_2/`)**

The results are written to CSV files in the `outputs/task_2` directory. The data shows the updated totals for each driver as new ride data is processed.

| driver_id | total_fare | average_distance  |
| --------- | ---------- | ----------------- |
| 7         | 335.30     | 23.3257           |
| 54        | 686.29     | 28.8580           |
| 15        | 431.89     | 15.4183           |
| 8         | 575.69     | 32.7000           |
| 2         | 796.81     | 27.6072           |
| ...       | ...        | ...               |

<br/>

---

## **Task 3: Windowed Time-Based Analytics**

1. Convert the timestamp column to a proper TimestampType.
2. Perform a 5-minute windowed aggregation on fare_amount (sliding by 1 minute and watermarking by 1 minute).

## **Instructions:**

1. Convert the string-based timestamp column to a TimestampType column (e.g., event_time).
2. Use Spark’s window function to aggregate over a 5-minute window, sliding by 1 minute, for the sum of fare_amount.
3. Output the windowed results to csv.

The `task3.py` script performs a time-based aggregation using a **5-minute sliding window** that advances every **1 minute**. It calculates the total `fare_amount` collected across all rides within each window. A 1-minute watermark is used to handle late-arriving data.

#### **Sample Output (`outputs/task_3/`)**

The results are written to CSV files in the `outputs/task_3` directory. Each row represents the total fare for a specific 5-minute window.

| window_start               | window_end                 | total_fare |
| -------------------------- | -------------------------- | ---------- |
| 2025-10-15T23:09:00.000Z   | 2025-10-15T23:14:00.000Z   | 22381.46   |
| ...                        | ...                        | ...        |

---

## 📬 Submission Checklist

- [x] Python scripts 
- [x] Output files in the `outputs/` directory  
- [x] Completed `README.md`  
- [x] Commit everything to GitHub Classroom  
- [x] Submit your GitHub repo link on canvas

---

