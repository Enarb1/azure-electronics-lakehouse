"""Reusable pipeline code for the Azure electronics lakehouse.

The Databricks job notebooks under ``notebooks/jobs/`` are deliberately
thin: they read their widgets and call into this package, so the logic
stays importable, reviewable in Git and runnable outside a notebook.

Layers:
    bronze: Auto Loader ingestion of the raw CSVs, append-only.
    silver: typing and cleaning, one table per entity.
    gold: the star schema the reports read.

Support modules:
    transform: column-name normalisation, shared by every layer.
    clean: per-entity type and cleaning rules, used by silver.
    quality: assertions that fail the build before a bad table lands.
"""
