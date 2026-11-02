# Crypto Warehouse

A cloud-native data engineering pipeline that collects cryptocurrency market data, validates and stores historical snapshots in a cloud data lake, and maintains an analytical warehouse for querying and reporting.

# Overview

This project was built to demonstrate the end-to-end lifecycle of a modern data pipeline:

Extracting data from external APIs
Performing validation and quality checks
Building historical datasets
Storing data in a cloud data lake
Loading data into an analytical warehouse
Containerising and deploying workloads to the cloud

The pipeline collects daily cryptocurrency market metrics from the CoinGecko API and stores them as historical snapshots for future analysis.

# Architecture
<img width="1280" height="853" alt="WhatsApp Image 2026-11-02 at 2 58 24 PM" src="https://github.com/user-attachments/assets/c0a6684b-3957-4f3f-a0dc-ce8c5bbc6e84" />