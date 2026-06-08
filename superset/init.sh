#!/bin/bash
# Registers the property_db PostgreSQL connection in Superset via its CLI
superset set-database-uri \
  -d "Property DB" \
  -u "postgresql+psycopg2://admin:admin123@postgres:5432/property_db"
