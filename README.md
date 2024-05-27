# Big_Data_E-commerce_Data_Processing_and_Analysis

This project demonstrates a complete pipeline for processing and analyzing e-commerce data in real-time using Big Data tools. The project is divided into two main parts: Batch Processing and Streaming Processing.

## Team Members

- Abdelrahman Mohamed 
- Abdullah Mohamed 
- Omnia Mostafa 

## Project Overview

### Batch Big Data Processing

We utilized a virtual machine (VM) that contains the following Big Data tools:
- **Hadoop**: For storing CSV files.
- **Spark**: For data analysis.
- **YARN**: For resource management.

#### Steps:

1. **Hadoop**:
   - Upload the dataset to the VM.
   - Copy data from local to HDFS using the command: `hdfs dfs -copyFromLocal local_path hdfs_path`.

2. **Spark**:
   - **Spark Submit**: Run pre-saved Python scripts using the `spark-submit` option.
   - **Jupyter**: Use Jupyter notebooks to run Spark code for testing and visualizing data.

### Streaming Big Data Processing

We extended our setup to include real-time data processing tools:
- **Kafka**: For real-time data ingestion.
- **Spark Streaming**: For real-time data processing.

#### Steps:

1. **Generate Fake Data**:
   - Create fake customer, product, and transaction data for streaming.

2. **Set up Kafka**:
   - Start Zookeeper and Kafka servers:
     ```bash
     ./zookeeper-server-start.sh ./../config/zookeeper.properties
     ./kafka-server-start.sh ./../config/server.properties
     ```

3. **Spark Streaming**:
   - Configure Spark to read data from Kafka topics (`ecommerce_customers`, `ecommerce_products`, etc.).
   - Perform data analysis on streaming data including filtering, aggregating, and enriching data.
   - Apply watermarking and windowing techniques to handle late data and segment data into time windows.

4. **Store Processed Data**:
   - Write the streaming data to HDFS in Parquet format using the `writeStream` API.

### Data Analysis Reports

- **Customer Analysis**: Gender report based on last login time.
- **Product Analysis**: Reports based on price and total stock.
- **Sales Analysis**: Reports on transactions and total sold quantity.

## Tools and Technologies

- **Hadoop**: For distributed storage.
- **Spark**: For data processing and analytics.
- **Kafka**: For real-time data ingestion.
- **Spark Streaming**: For processing streaming data.
- **Jupyter**: For interactive data analysis.

## Deployment

To deploy and run the project:

1. **Set up Hadoop**:
   - Ensure Hadoop is properly configured and running.
   - Copy your dataset to HDFS.

2. **Set up Kafka**:
   - Start Zookeeper and Kafka servers.

3. **Run Spark Jobs**:
   - Use `spark-submit` for batch processing scripts.
   - Use Jupyter notebooks for interactive analysis.
   - Configure and run Spark Streaming jobs to process data from Kafka.

4. **View Processed Data**:
   - Access the processed data stored in HDFS, typically in Parquet format.

## Contribution

Contributions are welcome! Please feel free to open issues or submit pull requests.

## License

This project is licensed under the MIT License.

---

Feel free to customize and expand upon this README file to suit the specific needs and details of your project.
