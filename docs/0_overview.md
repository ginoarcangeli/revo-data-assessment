# Data Pipeline Documentation

## Overview

The data pipeline is structured following the Medallion Architecture, where each layer serves a distinct purpose:
- **Bronze Layer**: Ingests raw data into a structured format.
- **Silver Layer**: Cleans and transforms data, enriching it with additional information.
- **Gold Layer**: Analyzes data to generate insights, focusing on average revenue per postcode.

The purpose of this pipeline is to identify postal codes better suited for investment, comparing long-term and short-term rental profitability.
