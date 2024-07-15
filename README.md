# Crisis Text Line Project

This project aims to implement a data processing pipeline based on the Medallion Architecture to analyze and process data from U.S. Department of Health and Human Services 

## Table of Contents
- [Data Sources](#data-sources)
- [Data Transformations](#data-transformations)
- [Medallion Architecture](#medallion-architecture)
- [Schema Design](#schema-design)
- [Installation and Usage](#installation-and-usage)
- [Contact](#contact)

## Data Source

The data for this project is sourced from the 2021 Mental Health Client-Level Data (MH-CLD) provided by the Substance Abuse and Mental Health Services Administration (SAMHSA). This dataset contains comprehensive information on mental health services utilization and client characteristics across the United States.

### Download and Description

- **Data File**: The data file can be downloaded from the following link:
  [MH-CLD-2021 Data CSV](https://www.datafiles.samhsa.gov/sites/default/files/field-uploads-protected/studies/MH-CLD-2021/MH-CLD-2021-datasets/MH-CLD-2021-DS0001/MH-CLD-2021-DS0001-bundles-with-study-info/MH-CLD-2021-DS0001-bndl-data-csv_v1.zip)
- **Codebook**: The codebook, which provides detailed information about the dataset's variables and their definitions, can be accessed here:
  [MH-CLD-2021 Codebook](https://www.datafiles.samhsa.gov/sites/default/files/field-uploads-protected/studies/MH-CLD-2021/MH-CLD-2021-datasets/MH-CLD-2021-DS0001/MH-CLD-2021-DS0001-info/MH-CLD-2021-DS0001-info-codebook.pdf)

### Data Description

The dataset includes the following key information:

- **Client Demographics**: Age, gender, race, ethnicity, and other demographic details.
- **Service Utilization**: Types and frequencies of mental health services utilized by clients.
- **Geographic Information**: Location data based on state and other regional identifiers.
- **Health Indicators**: Information on various health conditions and mental health diagnoses.

## Data Transformations

The data transformation process in this project follows the Medallion Architecture, which consists of three layers: Bronze, Silver, and Gold. Each layer represents a different stage of data processing, starting from raw data ingestion to final aggregated data ready for analysis.

### Data Transformation Steps Example

#### Bronze Layer

The Bronze Layer handles the initial ingestion of raw data. It performs basic data cleaning tasks to ensure the data is ready for further processing.

**Steps:**

1. **Read Raw Data**: Load the raw CSV data into a DataFrame using a predefined schema.
2. **Clean Data**:
   - Drop rows with all null values to ensure only relevant data is kept.
   - Remove duplicate rows to avoid redundancy.
3. **Write Cleaned Data**: Store the cleaned data into the Bronze layer partitioned by `STATEFIP`.

#### Silver Layer

The Silver Layer performs more advanced transformations, such as data type conversion and stratified sampling, to prepare the data for analysis and machine learning tasks.

**Steps:**

1. **Data Type Conversion**:
   - Convert specific columns from numeric to string types based on a configuration file.
   - Map numeric values to meaningful strings to make the data more interpretable.
2. **Stratified Sampling**:
   - Create a new column that combines the stratified columns to ensure proper sampling.
   - Split the data into training, testing, and validation sets based on the combined stratified column.
3. **Write Data**: Store the transformed data into the Silver layer, partitioned by the YEAR, GENDER, RACE, AGE, and STATEFIP.

#### Gold Layer

The Gold Layer aggregates the data and creates final datasets ready for analysis. This layer generates specific tables for different analytical purposes, such as aggregated services, health outcomes, and service utilization.

**Steps:**

1. **Create Aggregated Services Table**:
   - Aggregate the data by `YEAR`, `GENDER`, `RACE`, `AGE`, and `STATEFIP` to calculate the total services and service type count.
   - Store the aggregated data partitioned by `STATEFIP`.

2. **Create Health Outcomes Table**:
   - Select relevant columns for health outcomes analysis, such as `YEAR`, `AGE`, `GENDER`, `RACE`, `ETHNIC`, `STATEFIP`, `ANXIETYFLG`, and `DEPRESSFLG`.
   - Store the health outcomes data partitioned by `STATEFIP`.

3. **Create Service Utilization Table**:
   - Aggregate the data by `YEAR`, `EMPLOY`, and `STATEFIP` to calculate the total services and service type count.
   - Store the service utilization data partitioned by `STATEFIP`.

This structured approach ensures that data is progressively refined and enriched as it moves through each layer, culminating in high-quality datasets that are ready for detailed analysis and reporting.

## Medallion Architecture

The Medallion Architecture is a layered approach to data processing and storage that ensures data quality, scalability, and efficiency. This architecture consists of three primary layers: Bronze, Silver, and Gold. Each layer serves a specific purpose and progressively refines the data as it moves through the pipeline.

### Rationale

The Medallion Architecture is implemented to address several key requirements and challenges in data processing:

1. **Data Quality Management**:
   - **Incremental Data Improvement**: By processing data through multiple layers, each step in the pipeline adds a layer of data validation, cleaning, and transformation. This incremental approach ensures that the final datasets are of the highest quality.
   - **Error Isolation**: Issues can be isolated and addressed at the specific layer where they arise, making it easier to identify and fix data quality problems.

2. **Scalability and Performance**:
   - **Optimized Storage**: Each layer is optimized for specific types of storage and access patterns. The Bronze layer uses raw storage optimized for write operations, the Silver layer uses optimized storage for transformation and processing, and the Gold layer is designed for read-heavy analytical workloads.
   - **Efficient Processing**: The use of different layers allows for efficient processing of large datasets. Transformations can be done in stages, leveraging the power of distributed computing frameworks like Apache Spark.

3. **Flexibility and Extensibility**:
   - **Modular Design**: The architecture is modular, allowing for independent development and scaling of each layer. New data sources and processing steps can be easily integrated without disrupting the existing pipeline.
   - **Adaptability**: It can adapt to changing business requirements and data sources. As new types of data and processing requirements emerge, they can be incorporated into the appropriate layer.

4. **Security and Compliance**:
   - **Controlled Access**: Sensitive data can be protected by implementing security controls at each layer. Access can be restricted based on the data’s sensitivity and the user’s role.
   - **Compliance**: Ensuring compliance with data governance and regulatory requirements is more manageable as data is processed and stored in a controlled and auditable manner.

5. **Simplified Data Management**:
   - **Clear Data Lineage**: The transformation steps are well-defined, providing clear data lineage and traceability. This helps in understanding the data flow and transformations applied at each stage.
   - **Ease of Maintenance**: The layered approach simplifies maintenance tasks. Updates and changes can be applied to individual layers without affecting the entire pipeline.

### Implementation

#### Bronze Layer
- **Purpose**: Ingest and store raw data.
- **Processes**: Initial data load, basic cleaning (e.g., removing null values, duplicates).
- **Storage**: Optimized for write-heavy operations.

#### Silver Layer
- **Purpose**: Refine and transform data.
- **Processes**: Data type conversion, filtering, enrichment, stratified sampling.
- **Storage**: Optimized for transformation and processing.

#### Gold Layer
- **Purpose**: Aggregate and prepare data for analysis.
- **Processes**: Aggregation, creation of final analytical datasets, computation of key metrics.
- **Storage**: Optimized for read-heavy analytical operations.

By implementing the Medallion Architecture, we ensure that data processing is robust, scalable, and capable of delivering high-quality datasets ready for advanced analytics and business intelligence.
### Implementation

1. **Bronze Layer**: Stores raw data with initial cleaning.
2. **Silver Layer**: Performs transformations and filtering on bronze layer data.
3. **Gold Layer**: Stores final aggregated and analyzed data.

## Schema Design

The schema files are stored in the `document/schema` folder. Each file corresponds to a specific layer in the Medallion Architecture:

1. **Bronze Schema**:
    - **File Path**: [`document/schema/bronze`](document/schema/bronze)
    - **Description**: Defines the schema for the raw data ingestion layer. This schema is used to validate and clean the raw data as it is loaded into the Bronze layer.

2. **Silver Schema**:
    - **File Path**: [`document/schema/silver`](document/schema/silver)
    - **Description**: Defines the schema for the transformed data layer. This schema ensures that the data is correctly transformed and enriched after the initial cleaning in the Bronze layer.

3. **Gold Schema**:
    - **File Path**: [`document/schema/gold`](document/schema/gold)
    - **Description**: Defines the schema for the aggregated and final data layer. This schema ensures that the data is properly aggregated and formatted for analysis and reporting.

## Installation and Usage

### Installation

1. Clone this project:
    ```sh
    git clone https://github.com/mike840203/Crisis_Text_Line.git
    cd Crisis_Text_Line
    ```

2. Install dependencies:
    ```sh
    pip install -r requirements.txt
    ```

### Usage

1. Run data cleaning and transformation scripts:
    ```
    python notebooks/main.py
    ```
2. Edit table_type, data_type, table_name in the test/run_test.py file to load the data:
   ```
   test.load_data(table_type, data_type, table_name)

   table_type = ''  # Change to 'bronze', 'silver', or 'gold'
   data_type = ''   # Change to 'training', 'testing', 'validation' or leave empty for bronze
   table_name = ''  # Change to 'aggregated_services', 'health_outcomes', 'service_utilization' or leave empty for bronze/silver
   ```
3. Edit SQL in test/query.sql
4. run the test/run_test.py file to query the data:
   ```
   python test/run_test.py
   ```


## Contact

If you have any questions, please contact [mike410123024@gmail.com](mailto:mike410123024@gmail.com).