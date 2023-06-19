from __future__ import annotations

import os

CADS_API_URL = os.getenv("CADS_API_URL", "http://localhost:8080/api")
CADS_API_KEY = os.getenv("CADS_API_KEY")

CATALOGUE_DIR = "catalogue"
RETRIEVE_DIR = "retrieve"
PROFILES_DIR = "profiles"

API_VERSION = 1
