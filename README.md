# ✈️ Aviation Data ETL Pipeline 🛫

&nbsp;

## Table of Contents
- [Overview](#overview)
- [Prerequisites](#prerequisites)
- [Installation Steps](#installation-steps)
- [Running the Pipeline](#running-the-pipeline)
- [Monitoring & Verification](#monitoring--verification)
- [Troubleshooting](#troubleshooting)
- [Contact](#contact)

&nbsp;

## Overview 📋
This pipeline processes aviation data using Apache Cassandra for storage and Apache Spark for data transformation. The system handles flight information and makes it available for analytics through CQL queries.

&nbsp;

## Prerequisites 🔧
- Apache Cassandra 4.0.0
- Apache Spark
- Python
- Required Python packages (spark-cassandra-connector)

&nbsp;

## Installation Steps 🚀

### 1. Setting Up Cassandra 💾

Navigate to Cassandra directory:
```bash
cd /apache-cassandra-4.0.0
```

&nbsp;

## Running the Pipeline 🔄

### Step 1: Start Cassandra Server 🟢
```bash
# Navigate to Cassandra directory
cd /apache-cassandra-4.0.0

# Start Cassandra in foreground mode
bin/cassandra -f
```

&nbsp;

### Step 2: Run ETL Pipeline 🔄
Open a new terminal and run:
```bash
# Submit Spark job
spark-submit --packages com.datastax.spark:spark-cassandra-connector_2.12:3.2.0 \
/home/sandheep/airflow_home/dags/scripts/sky_data_etl.py
```

&nbsp;

### Step 3: Verify Results in CQL 🔍
Open a new terminal and follow these steps:

```bash
# Navigate to Cassandra directory
cd /apache-cassandra-4.0.0
cd bin

# Start CQL shell
python -m cqlsh

# Switch to aviation keyspace
USE aviation;

# View processed data
SELECT * FROM flights LIMIT 10;
```

&nbsp;

## Quick Commands Reference 📝

### Cassandra Commands 💻
```sql
-- Switch keyspace
USE aviation;

-- View table contents
SELECT * FROM flights LIMIT 10;

-- Check table structure
DESCRIBE TABLE flights;
```

&nbsp;

## Troubleshooting 🔧

### Common Issues and Solutions:

1. **Cassandra Connection Issues** 🔴
   - Verify Cassandra is running: Check for active process
   - Ensure correct port configuration
   - Check logs in Cassandra's log directory

2. **Spark Submit Errors** ⚠️
   - Verify path to ETL script is correct
   - Check if all dependencies are properly installed
   - Ensure Cassandra connector version matches Spark version

3. **CQL Connection Issues** 🔌
   - Ensure Cassandra is running
   - Verify you're in the correct directory
   - Check Python environment

&nbsp;

## Best Practices 💡
- Always verify Cassandra is running before starting ETL
- Monitor the ETL process for any errors
- Use LIMIT when querying tables initially
- Keep Cassandra logs accessible for troubleshooting

&nbsp;

## Need Help? 🆘
If you encounter any issues:
1. Check Cassandra logs
2. Verify all prerequisites are installed
3. Ensure all paths are correct
4. Monitor Spark job progress

&nbsp;

## Contact Me 📫

### Let's Connect! 🤝
- **GitHub**: [SANDHEEP S](https://github.com/Sandheep-S-95)
- **LinkedIn**: [SANDHEEP S](https://www.linkedin.com/in/sandheep-s-868a55284/)
- **Email**: sand.s.heep95@gmail.com

### Additional Resources 📚
- Project Documentation
- Blog Posts
- Video Tutorials

&nbsp;

### Support This Project ⭐
If you found this project helpful, consider:
- Giving it a star on GitHub
- Sharing it with others
- Contributing to its development
- Reporting issues or suggesting improvements

&nbsp;

---
Happy Data Processing! 🎉

