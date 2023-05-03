# HertzPySpark
## Setup and verify the solution

The following instructions will guide you to set up and verify the solution:

### Prerequisites

* Python 3.6 or later
* Apache Spark 2.4.0 or later

### Installation

1. Clone the repository:
```
git clone https://github.com/Aishboom/HertzPySpark.git
```
2. Create and activate a virtual environment:
```
python3 -m venv venv
source venv/bin/activate
```
3. Install the required packages:
```
pip install -r requirements.txt
```

### Usage

1. Specify the path to the folder containing JSON files and the output path in the `Hertz_Data_Procesing.py` file:
```python
folder_path = "events/"
output_path = "file:/path/output"
```

2. Run the script:
```
python Hertz_Data_Procesing.py
```

### Verification

1. Check that the output files are generated in the output path specified in the `Hertz_Data_Procesing.py` file.

2. Verify that the output files have the expected schema and data.

3. To ensure the partition hierarchy, the table_name(diagnostic_events_output and location_events_output) should be the first level, 
   followed by the date(date = date=1980-01-01), and then by hour(Hour=0). 
   Additionally, the number of buckets mentioned is 2. The choice of the number of buckets is generally based on the number 
   of `executor_instances*executor_cores`. This partitioning scheme allows for efficient querying of the data based on the date and time, 
   while the number of buckets ensures that the data is evenly distributed across the cluster for parallel processing.

4. If you encounter any errors, please check the logs in the console or the log files in the logs directory.