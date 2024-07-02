# AWS Data Lake Project

This project sets up a scalable data lake using AWS services such as S3, Glue, Athena, SageMaker, and Lambda. It includes infrastructure as code with CloudFormation, data processing scripts with AWS Glue, data quality checks with Great Expectations, and machine learning workflows with SageMaker.

## Table of Contents
1. [Project Overview](#project-overview)
2. [Setup](#setup)
3. [Usage](#usage)
4. [Contributing](#contributing)
5. [License](#license)

## Project Overview
This project aims to create a fully functional data lake on AWS, enabling efficient data storage, processing, and analysis.

## Setup
Please refer to the [Setup Guide](docs/setup-guide.md) for detailed installation instructions.

## Usage
- **Data Ingestion**: Use Glue scripts in `src/glue/scripts` to ingest data.
- **Data Processing**: Process data with dbt models located in `src/dbt/models`.
- **Data Quality**: Validate data using Great Expectations configurations in `src/great_expectations`.

## Contributing
Please read our [Contributing Guide](CONTRIBUTING.md) for details on how to contribute to this project.

## License
This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.
