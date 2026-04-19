"""
Raw layer models / contracts.

Purpose:
- define the expected structure of raw data before any business transformations
- act as a boundary between extract and staging layers

Current state:
- raw data is stored as a flat DataFrame exported to CSV
- no explicit schema enforcement is applied at this stage

Future extensions:
- schema validation (e.g. pandera / pydantic)
- typed data models
- support for alternative storage (e.g. Parquet, database)
"""